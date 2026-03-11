package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sqs-movie-consumer/consumer"
	"sqs-movie-consumer/storage"
	"sqs-movie-consumer/transcoder"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// --- S3 Event structs ---

// --- Resolution config ---

type Resolution struct {
	Name      string
	Width     int
	Height    int
	Bitrate   string
	Bandwidth int // for master playlist, in bits/s
}

var resolutions = []Resolution{
	{Name: "360p", Width: 640, Height: 360, Bitrate: "800k", Bandwidth: 800000},
	{Name: "720p", Width: 1280, Height: 720, Bitrate: "2800k", Bandwidth: 2800000},
	{Name: "1080p", Width: 1920, Height: 1080, Bitrate: "5000k", Bandwidth: 5000000},
}

// --- FFmpeg HLS per resolution ---

func generateHLSForResolution(inputFile, outputDir, baseName string, res Resolution) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output dir: %w", err)
	}

	playlistPath := filepath.Join(outputDir, "index.m3u8")

	cmd := exec.Command("ffmpeg",
		"-i", inputFile,
		"-c:v", "libx264",
		"-c:a", "aac",
		"-preset", "fast",
		"-crf", "23",
		"-vf", fmt.Sprintf("scale=%d:%d", res.Width, res.Height),
		"-b:v", res.Bitrate,
		"-f", "hls",
		"-hls_time", "10",
		"-hls_playlist_type", "vod",
		"-hls_segment_filename", filepath.Join(outputDir, "seg%d.ts"),
		playlistPath,
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Printf("Generating HLS for %s...\n", res.Name)
	return cmd.Run()
}

// --- Master playlist ---

func generateMasterPlaylist(resolutions []Resolution, baseName string) string {
	var sb strings.Builder
	sb.WriteString("#EXTM3U\n")
	sb.WriteString("#EXT-X-VERSION:3\n\n")

	for _, res := range resolutions {
		sb.WriteString(fmt.Sprintf("#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d\n",
			res.Bandwidth, res.Width, res.Height))
		sb.WriteString(fmt.Sprintf("%s/index.m3u8\n", res.Name))
	}

	return sb.String()
}

// --- Business logic ---

//func handleS3MultipartComplete(ctx context.Context, s3Storage *storage.S3Storage, event *S3EventNotification) error {
//	for _, record := range event.Records {
//		if record.EventName != "ObjectCreated:CompleteMultipartUpload" {
//			fmt.Println("Skipping unrelated event:", record.EventName)
//			continue
//		}
//
//		bucket := record.S3.Bucket.Name
//		key := record.S3.Object.Key
//
//		fmt.Println("Downloading from S3:", bucket, key)
//		videoBytes, err := s3Storage.DownloadLargeFile(bucket, key)
//		if err != nil {
//			fmt.Println("Error downloading file:", err)
//			continue
//		}
//
//		// Preserve original extension — don't assume .mp4
//		originalExt := filepath.Ext(filepath.Base(key))
//		baseName := strings.TrimSuffix(filepath.Base(key), originalExt)
//
//		// Temp dir
//		tmpDir, err := os.MkdirTemp("", "video_hls_")
//		if err != nil {
//			fmt.Println("Failed to create temp dir:", err)
//			continue
//		}
//
//		// Write input file with its original extension
//		inputFile := filepath.Join(tmpDir, "input"+originalExt)
//		if err := os.WriteFile(inputFile, videoBytes, 0644); err != nil {
//			fmt.Println("Failed to write temp video file:", err)
//			os.RemoveAll(tmpDir)
//			continue
//		}
//
//		// Generate HLS for each resolution
//		successfulResolutions := []Resolution{}
//		for _, res := range resolutions {
//			resDir := filepath.Join(tmpDir, res.Name)
//			if err := generateHLSForResolution(inputFile, resDir, baseName, res); err != nil {
//				fmt.Printf("FFmpeg failed for %s: %v — skipping\n", res.Name, err)
//				continue
//			}
//			successfulResolutions = append(successfulResolutions, res)
//		}
//
//		if len(successfulResolutions) == 0 {
//			fmt.Println("All resolutions failed, skipping upload")
//			os.RemoveAll(tmpDir)
//			continue
//		}
//
//		// Generate and upload master playlist
//		masterPlaylist := generateMasterPlaylist(successfulResolutions, baseName)
//		masterKey := fmt.Sprintf("%s/master.m3u8", baseName)
//		if err := s3Storage.UploadFile("movies", masterKey, []byte(masterPlaylist)); err != nil {
//			fmt.Println("Failed to upload master playlist:", err)
//		} else {
//			fmt.Println("Uploaded master playlist:", masterKey)
//		}
//
//		// Upload all resolution folders
//		for _, res := range successfulResolutions {
//			resDir := filepath.Join(tmpDir, res.Name)
//			files, err := os.ReadDir(resDir)
//			if err != nil {
//				fmt.Printf("Failed to read dir for %s: %v\n", res.Name, err)
//				continue
//			}
//
//			for _, f := range files {
//				if f.IsDir() {
//					continue
//				}
//
//				filePath := filepath.Join(resDir, f.Name())
//				data, err := os.ReadFile(filePath)
//				if err != nil {
//					fmt.Printf("Failed to read %s: %v\n", f.Name(), err)
//					continue
//				}
//
//				s3Key := fmt.Sprintf("%s/%s/%s", baseName, res.Name, f.Name())
//				if err := s3Storage.UploadFile("movies", s3Key, data); err != nil {
//					fmt.Printf("Failed to upload %s: %v\n", s3Key, err)
//				} else {
//					fmt.Println("Uploaded:", s3Key)
//				}
//			}
//		}
//
//		os.RemoveAll(tmpDir)
//		fmt.Println("Processing complete for:", key)
//	}
//
//	return nil
//}

// TODO: clean the code
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
