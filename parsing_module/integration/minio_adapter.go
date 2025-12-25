package integration

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"json_parser_module/utils"
)

type MinioAdapter struct {
	client        *minio.Client
	defaultBucket string
	endpoint      string
}

func (adapter *MinioAdapter) ensureBucket(ctx context.Context, bucket string) error {
	if bucket == "" {
		return fmt.Errorf("Bucket name is required")
	}

	exists, err := adapter.client.BucketExists(ctx, bucket)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	return adapter.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
}

func NewMinioAdapter() *MinioAdapter {
	var endpoint = utils.GetEnv("MINIO_ENDPOINT", "minio:9000")
	var accessKey = utils.GetEnv("MINIO_ACCESS_KEY", "minioadmin")
	var secretKey = utils.GetEnv("MINIO_SECRET_KEY", "minioadmin")
	var bucket = utils.GetEnv("MINIO_BUCKET", "output-files")

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})

	if err != nil {
		log.Printf("Unable minio connect %s: %v", bucket, err)
		return nil
	}

	svc := &MinioAdapter{
		client:        client,
		defaultBucket: bucket,
		endpoint:      endpoint,
	}

	if bucket != "" {
		if err := svc.ensureBucket(context.Background(), bucket); err != nil {
			log.Fatalf("Unable to ensure bucket %s: %v", bucket, err)
		}
	}

	return svc
}

func (adapter *MinioAdapter) GetBucket() string {
	return adapter.defaultBucket
}

func (adapter *MinioAdapter) GetEndpoint() string {
	return adapter.endpoint
}

// Чтение файла из minio
func (adapter *MinioAdapter) GetFileAsBytes(bucket, file_name string) ([]byte, error) {
	reader, err := adapter.client.GetObject(context.Background(), bucket, file_name, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(reader)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Удаление файла из minio
func (adapter *MinioAdapter) RemoveFile(bucket, file_name string) error {
	opts := minio.RemoveObjectOptions{
		GovernanceBypass: true,
	}

	err := adapter.client.RemoveObject(context.Background(), bucket, file_name, opts)
	if err != nil {
		return err
	}

	return nil
}

// Отправка файла в minio
func (adapter *MinioAdapter) UploadObject(objectName string, content []byte, contentType string) (string, error) {
	if objectName == "" {
		return "", fmt.Errorf("Object name is required")
	}

	if err := adapter.ensureBucket(context.Background(), adapter.defaultBucket); err != nil {
		return "", err
	}

	reader := bytes.NewReader(content)
	info, err := adapter.client.PutObject(context.Background(), adapter.defaultBucket, objectName, reader, reader.Size(), minio.PutObjectOptions{
		ContentType: contentType,
	})

	if err != nil {
		return "", err
	}

	return info.ChecksumSHA256, nil
}
