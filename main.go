package main

import (
	"context"
	"fmt"
	"os"
	"sqs-movie-consumer/consumer"
	"sqs-movie-consumer/storage"
	"sqs-movie-consumer/transcoder"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background(),
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

	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String("http://localhost:4566")
	})

	c := &consumer.Consumer{
		Client:    sqsClient,
		QueueName: "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/upload-events",
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:4566")
		o.UsePathStyle = true
	})
	s3Storage := storage.NewS3Storage(s3Client)

	transcoder := transcoder.New(s3Storage, "movies")

	consumer.StartPool(transcoder.Handle, c)
}
