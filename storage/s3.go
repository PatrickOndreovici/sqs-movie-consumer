package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3EventNotification struct {
	Records []S3EventRecord `json:"Records"`
}

type S3EventRecord struct {
	EventSource string   `json:"eventSource"`
	EventName   string   `json:"eventName"`
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
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

type S3Storage struct {
	S3Client *s3.Client
}

func NewS3Storage(s3Client *s3.Client) *S3Storage {
	return &S3Storage{
		S3Client: s3Client,
	}
}

// download large file
func (s *S3Storage) DownloadLargeFile(bucket, key string) ([]byte, error) {
	var partMiBs int64 = 10
	downloader := manager.NewDownloader(s.S3Client, func(d *manager.Downloader) {
		d.PartSize = partMiBs * 1024 * 1024
	})

	buffer := manager.NewWriteAtBuffer([]byte{})
	_, err := downloader.Download(context.Background(), buffer, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})

	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// upload file to S3
func (s *S3Storage) UploadFile(bucket, key string, data []byte) error {
	uploader := manager.NewUploader(s.S3Client)

	_, err := uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("failed to upload file to s3: %w", err)
	}
	return nil
}
