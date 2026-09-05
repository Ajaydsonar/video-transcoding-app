package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
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

func (b *B2) Delete(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("storage: b2 delete %s: %w", key, err)
	}
	return nil
}

// DeletePrefix lists every object under prefix and deletes them in
// batches. A prefix matching nothing is success — ListObjectsV2 simply
// returns no contents and we skip the delete call entirely.
func (b *B2) DeletePrefix(ctx context.Context, prefix string) error {
	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(prefix),
	})

	var batch []types.ObjectIdentifier
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		_, err := b.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(b.bucket),
			Delete: &types.Delete{Objects: batch},
		})
		if err != nil {
			return fmt.Errorf("storage: b2 delete prefix %s: %w", prefix, err)
		}
		batch = batch[:0]
		return nil
	}

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("storage: b2 list prefix %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			batch = append(batch, types.ObjectIdentifier{Key: obj.Key})
			if len(batch) == 1000 {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}

// PresignPut hands the client a URL it can PUT key to directly. The
// signature covers bucket + key only, so the browser may send any
// Content-Type — the complete endpoint re-validates size and content
// server-side before the job is queued.
func (b *B2) PresignPut(ctx context.Context, key string, expiry time.Duration) (string, error) {
	pc := s3.NewPresignClient(b.client)
	out, err := pc.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("storage: b2 presign put %s: %w", key, err)
	}
	return out.URL, nil
}

// Stat issues a HEAD: cheap existence + size check without fetching the
// object. A missing key maps to ErrNotFound so callers can answer "not
// uploaded yet" with a 4xx instead of a 500.
func (b *B2) Stat(ctx context.Context, key string) (int64, error) {
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey") {
			return 0, fmt.Errorf("storage: stat %s: %w", key, ErrNotFound)
		}
		return 0, fmt.Errorf("storage: b2 stat %s: %w", key, err)
	}
	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return size, nil
}

var _ Storage = (*B2)(nil)
