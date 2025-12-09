package integration

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	. "json_parser_module/utils"
)

type MinioAdapter struct {
	client        *minio.Client
	defaultBucket string
}

func (adapter *MinioAdapter) ensureBucket(context context.Context, bucket string) error {
	if bucket == "" {
		return fmt.Errorf("Bucket name is required")
	}

	exists, err := adapter.client.BucketExists(context, bucket)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	return adapter.client.MakeBucket(context, bucket, minio.MakeBucketOptions{})
}

func NewMinioAdapter() *MinioAdapter {
	var endpoint = GetEnv("MINIO_ENDPOINT", "localhost:9000")
	var accessKey = GetEnv("MINIO_ACCESS_KEY", "minioadmin")
	var secretKey = GetEnv("MINIO_SECRET_KEY", "minioadmin")
	var bucket = GetEnv("MINIO_BUCKET", "telegram")

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})

	if err != nil {
		go log.Printf("Unable minio connect %s: %v", bucket, err)
		return nil
	}

	svc := &MinioAdapter{
		client:        client,
		defaultBucket: bucket,
	}

	if bucket != "" {
		if err := svc.ensureBucket(context.Background(), bucket); err != nil {
			log.Fatalf("Unable to ensure bucket %s: %v", bucket, err)
		}
	}

	return svc
}

func (adapter *MinioAdapter) GetFileAsBytes(bucket, file_name string) []byte {
	reader, err := adapter.client.GetObject(context.Background(), bucket, file_name, minio.GetObjectOptions{})
	if err != nil {
		log.Fatalln(err)
	}
	defer reader.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(reader)
	if err != nil {
		log.Fatalln(err)
	}

	return buf.Bytes()
}

func (adapter *MinioAdapter) RemoveFile(bucket, file_name string) error {
	opts := minio.RemoveObjectOptions{
		GovernanceBypass: true,
	}

	err := adapter.client.RemoveObject(context.Background(), bucket, file_name, opts)
	if err != nil {
		log.Fatalln(err)
		return err
	}

	return nil
}
