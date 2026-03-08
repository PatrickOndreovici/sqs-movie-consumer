package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Consumer struct {
	Client    *sqs.Client
	QueueName string
}

func StartPool[T any](serviceFunc func(ctx context.Context, dto *T) error, consumer *Consumer) {
	ctx := context.Background()

	params := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: 10,
		QueueUrl:            aws.String(consumer.QueueName),
		WaitTimeSeconds:     20,
		VisibilityTimeout:   30,
		MessageAttributeNames: []string{
			string(types.QueueAttributeNameAll),
		},
	}

	msgCh := make(chan types.Message, 20)
	var wg sync.WaitGroup

	startWorkers(ctx, msgCh, &wg, consumer, serviceFunc)

	for {
		resp, err := consumer.Client.ReceiveMessage(ctx, params)

		if err != nil {
			fmt.Println(err)
			continue
		}

		for _, msg := range resp.Messages {
			wg.Add(1)
			msgCh <- msg
		}

	}
}

func startWorkers[T any](
	ctx context.Context,
	msgCh chan types.Message,
	wg *sync.WaitGroup,
	consumer *Consumer,
	serviceFunc func(ctx context.Context, dto *T) error,
) {
	processingMessages := &sync.Map{} // tracks in-flight message IDs to avoid duplicates

	for i := 0; i < 10; i++ { // fixed pool of 10 workers
		go worker(ctx, msgCh, wg, consumer, processingMessages, serviceFunc)
	}
}

func worker[T any](
	ctx context.Context,
	msgCh chan types.Message,
	wg *sync.WaitGroup,
	consumer *Consumer,
	processingMessages *sync.Map,
	serviceFunc func(ctx context.Context, dto *T) error,
) {
	for msg := range msgCh { // each worker waits for messages from the channel
		// Deduplicate: skip if another worker is already handling this message ID
		if _, alreadyProcessing := processingMessages.LoadOrStore(*msg.MessageId, true); alreadyProcessing {
			fmt.Println("Duplicate message, skipping:", *msg.MessageId)
			wg.Done()
			continue
		}

		// Parse the message body into your struct
		var dto T
		if err := json.Unmarshal([]byte(*msg.Body), &dto); err != nil {
			fmt.Println("Failed to parse message body:", err)
			processingMessages.Delete(*msg.MessageId)
			wg.Done()
			continue
		}

		// Call your business logic
		if err := serviceFunc(ctx, &dto); err != nil {
			fmt.Println("Error processing message:", err)
			// Don't delete from SQS — let visibility timeout expire so it retries
		} else {
			// Success: delete the message from the queue so it's not redelivered
			_, err := consumer.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(consumer.QueueName),
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				fmt.Println("Failed to delete message:", err)
			} else {
				fmt.Println("Successfully processed and deleted message:", *msg.MessageId)
			}
		}

		processingMessages.Delete(*msg.MessageId)
		wg.Done()
	}
}
