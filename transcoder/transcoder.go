package transcoder

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sqs-movie-consumer/storage"
	"strconv"
	"strings"
)

type Resolution struct {
	Name      string
	Width     int
	Height    int
	Bitrate   string
	Bandwidth int
}

var defaultResolutions = []Resolution{
	{Name: "360p", Width: 640, Height: 360, Bitrate: "800k", Bandwidth: 800000},
	{Name: "720p", Width: 1280, Height: 720, Bitrate: "2800k", Bandwidth: 2800000},
	{Name: "1080p", Width: 1920, Height: 1080, Bitrate: "5000k", Bandwidth: 5000000},
}

type Transcoder struct {
	Storage     *storage.S3Storage
	Bucket      string
	Resolutions []Resolution
}

func New(s *storage.S3Storage, bucket string) *Transcoder {
	return &Transcoder{
		Storage:     s,
		Bucket:      bucket,
		Resolutions: defaultResolutions,
	}
}

func (t *Transcoder) Handle(event *storage.S3EventNotification) error {
	for _, record := range event.Records {
		if record.EventName != "ObjectCreated:CompleteMultipartUpload" {
			fmt.Println("Skipping unrelated event:", record.EventName)
			continue
		}

		if err := t.processRecord(record); err != nil {
			fmt.Printf("Failed to process record %s: %v\n", record.S3.Object.Key, err)
		}
	}
	return nil
}

func (t *Transcoder) processRecord(record storage.S3EventRecord) error {
	bucket := record.S3.Bucket.Name
	key := record.S3.Object.Key

	fmt.Println("Downloading from S3:", bucket, key)
	videoBytes, err := t.Storage.DownloadLargeFile(bucket, key)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}

	originalExt := filepath.Ext(filepath.Base(key))
	baseName := strings.TrimSuffix(filepath.Base(key), originalExt)

	// Detect source resolution via pipe — no temp file needed for probing
	videoInfo, err := t.getVideoInfo(videoBytes)
	if err != nil {
		return fmt.Errorf("failed to detect resolution: %w", err)
	}
	fmt.Printf("Source resolution: %dx%d\n", videoInfo.Width, videoInfo.Height)

	// Single temp dir for input file and all HLS output
	tmpDir, err := os.MkdirTemp("/tmp", "video_hls_*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	inputFile := filepath.Join(tmpDir, "input"+originalExt)
	if err := os.WriteFile(inputFile, videoBytes, 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	// Transcode each applicable resolution
	var successful []Resolution
	for _, res := range t.applicableResolutions(videoInfo) {
		if err := t.generateHLS(inputFile, tmpDir, res); err != nil {
			fmt.Printf("FFmpeg failed for %s: %v — skipping\n", res.Name, err)
			continue
		}
		successful = append(successful, res)
	}

	if len(successful) == 0 {
		return fmt.Errorf("all resolutions failed for %s", key)
	}

	// Upload master playlist
	master := t.generateMasterPlaylist(successful)
	masterKey := fmt.Sprintf("%s/master.m3u8", baseName)
	if err := t.Storage.UploadFile(t.Bucket, masterKey, []byte(master)); err != nil {
		return fmt.Errorf("failed to upload master playlist: %w", err)
	}
	fmt.Println("Uploaded master playlist:", masterKey)

	// Upload segments per resolution
	for _, res := range successful {
		if err := t.uploadResolution(tmpDir, baseName, res); err != nil {
			fmt.Printf("Failed to upload %s: %v\n", res.Name, err)
		}
	}

	fmt.Println("Processing complete for:", key)
	return nil
}

// generateHLS transcodes the input file into HLS segments for the given resolution.
// Each resolution gets its own subdirectory inside outputDir to avoid filename collisions.
func (t *Transcoder) generateHLS(inputFile, outputDir string, res Resolution) error {
	resDir := filepath.Join(outputDir, res.Name)
	if err := os.MkdirAll(resDir, 0755); err != nil {
		return fmt.Errorf("failed to create resolution dir: %w", err)
	}

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
		"-hls_segment_filename", filepath.Join(resDir, "seg%d.ts"),
		filepath.Join(resDir, "index.m3u8"),
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Printf("Transcoding %s...\n", res.Name)

	return cmd.Run()
}

func (t *Transcoder) uploadResolution(tmpDir, baseName string, res Resolution) error {
	resDir := filepath.Join(tmpDir, res.Name)
	files, err := os.ReadDir(resDir)
	if err != nil {
		return fmt.Errorf("failed to read dir: %w", err)
	}

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		data, err := os.ReadFile(filepath.Join(resDir, f.Name()))
		if err != nil {
			fmt.Printf("Failed to read %s: %v\n", f.Name(), err)
			continue
		}

		s3Key := fmt.Sprintf("%s/%s/%s", baseName, res.Name, f.Name())
		if err := t.Storage.UploadFile(t.Bucket, s3Key, data); err != nil {
			fmt.Printf("Failed to upload %s: %v\n", s3Key, err)
		} else {
			fmt.Println("Uploaded:", s3Key)
		}
	}
	return nil
}

func (t *Transcoder) generateMasterPlaylist(resolutions []Resolution) string {
	var sb strings.Builder
	sb.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n\n")
	for _, res := range resolutions {
		sb.WriteString(fmt.Sprintf("#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d\n%s/index.m3u8\n",
			res.Bandwidth, res.Width, res.Height, res.Name))
	}
	return sb.String()
}

type videoInfo struct {
	Width  int
	Height int
}

// getVideoInfo probes video dimensions by piping bytes into ffprobe via stdin.
func (t *Transcoder) getVideoInfo(videoBytes []byte) (*videoInfo, error) {
	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "stream=width,height",
		"-of", "csv=p=0",
		"-i", "pipe:0",
	)
	cmd.Stdin = bytes.NewReader(videoBytes)

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe failed: %w", err)
	}

	parts := strings.Split(strings.TrimSpace(string(out)), ",")
	if len(parts) != 2 {
		return nil, fmt.Errorf("unexpected ffprobe output: %s", string(out))
	}

	w, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, err
	}
	h, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}

	return &videoInfo{Width: w, Height: h}, nil
}

func (t *Transcoder) applicableResolutions(info *videoInfo) []Resolution {
	var result []Resolution
	for _, res := range t.Resolutions {
		if res.Height <= info.Height {
			result = append(result, res)
		}
	}
	if len(result) == 0 {
		result = append(result, t.Resolutions[0])
	}
	return result
}
