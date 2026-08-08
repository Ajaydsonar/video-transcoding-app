package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type B2 struct {
	client *s3.Client
	bucket string
}

type B2Config struct {
	Endpoint       string
	Region         string
	Bucket         string
	KeyID          string
	ApplicationKey string
}

func NewB2(ctx context.Context, cfg B2Config) (*B2, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.KeyID, cfg.ApplicationKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("storage: loading aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = &cfg.Endpoint
	})

	return &B2{client: client, bucket: cfg.Bucket}, nil
}

func (b *B2) Put(ctx context.Context, key string, r io.Reader) error {
	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	if err != nil {
		return fmt.Errorf("storage: b2 put %s: %w", key, err)
	}

	return nil
}

func (b *B2) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &b.bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, fmt.Errorf("storage: b2 get %s: %w", key, err)
	}

	return out.Body, nil
}

var _ Storage = (*B2)(nil)
