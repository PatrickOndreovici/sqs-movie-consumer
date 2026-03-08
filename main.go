package main

import (
	"context"
	"fmt"
	"os"
	"sqs-movie-consumer/consumer"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// --- S3 Event structs (matches the JSON envelope S3 sends) ---

type S3EventNotification struct {
	Records []S3EventRecord `json:"Records"`
}

type S3EventRecord struct {
	EventSource string   `json:"eventSource"`
	EventName   string   `json:"eventName"` // "ObjectCreated:CompleteMultipartUpload"
	S3          S3Detail `json:"s3"`
}

type S3Detail struct {
	Bucket S3Bucket `json:"bucket"`
	Object S3Object `json:"object"`
}

type S3Bucket struct {
	Name string `json:"name"`
}

type S3Object struct {
	Key  string `json:"key"`  // file path in the bucket
	Size int64  `json:"size"` // size in bytes
}

// --- Your business logic ---

func handleS3MultipartComplete(ctx context.Context, event *S3EventNotification) error {
	for _, record := range event.Records {
		// Guard: only handle multipart completion events
		if record.EventName != "ObjectCreated:CompleteMultipartUpload" {
			fmt.Println("Skipping unrelated event:", record.EventName)
			continue
		}

		bucket := record.S3.Bucket.Name
		key := record.S3.Object.Key
		size := record.S3.Object.Size

		fmt.Printf("Multipart upload complete — bucket=%s, key=%s, size=%d bytes\n",
			bucket, key, size)

	}

	return nil // return error to prevent deletion and trigger retry
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		// Fake credentials — LocalStack doesn't validate these
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"fake-access-key",
			"fake-secret-key",
			"",
		)),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		fmt.Println("Failed to load AWS config:", err)
		os.Exit(1)
	}

	// Point to LocalStack instead of real AWS
	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String("http://localhost:4566")
	})

	c := &consumer.Consumer{
		Client:    sqsClient,
		QueueName: "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/upload-events",
	}

	consumer.StartPool(handleS3MultipartComplete, c)
}
