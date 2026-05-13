package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const defaultS3KeyPrefix = "gh-actions-cache"

type S3Adapter struct {
	client    *s3.Client
	presign   *s3.PresignClient
	bucket    string
	keyPrefix string
}

func NewS3Adapter(ctx context.Context, cfg config.StorageConfig) (*S3Adapter, error) {
	if cfg.S3Bucket == "" {
		return nil, fmt.Errorf("STORAGE_S3_BUCKET is required")
	}

	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.S3Region),
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		options.UsePathStyle = cfg.S3ForcePathStyle
		if cfg.S3EndpointURL != "" {
			options.BaseEndpoint = aws.String(cfg.S3EndpointURL)
		}
	})

	return &S3Adapter{
		client:    client,
		presign:   s3.NewPresignClient(client),
		bucket:    cfg.S3Bucket,
		keyPrefix: s3KeyPrefix(cfg.S3KeyPrefix),
	}, nil
}

func (a *S3Adapter) UploadStream(ctx context.Context, objectName string, stream io.Reader) error {
	_, err := a.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(a.key(objectName)),
		Body:   stream,
	})
	if err != nil {
		return fmt.Errorf("put s3 object: %w", err)
	}
	return nil
}

func (a *S3Adapter) CreateDownloadStream(ctx context.Context, objectName string) (io.ReadCloser, error) {
	output, err := a.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(a.key(objectName)),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, ObjectNotFoundError{ObjectName: objectName}
		}
		return nil, fmt.Errorf("get s3 object: %w", err)
	}
	return output.Body, nil
}

func (a *S3Adapter) DeleteFolder(ctx context.Context, folderName string) error {
	return a.deleteByPrefix(ctx, a.key(folderName)+"/")
}

func (a *S3Adapter) CountFilesInFolder(ctx context.Context, folderName string) (int, error) {
	prefix := a.key(folderName) + "/"
	paginator := s3.NewListObjectsV2Paginator(a.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(a.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	count := 0
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("list s3 objects: %w", err)
		}
		for _, object := range page.Contents {
			if object.Key != nil && isDirectS3FolderObject(prefix, aws.ToString(object.Key)) {
				count++
			}
		}
	}
	return count, nil
}

func (a *S3Adapter) Clear(ctx context.Context) error {
	return a.deleteByPrefix(ctx, a.clearPrefix())
}

func (a *S3Adapter) CreateDownloadURL(ctx context.Context, objectName string, ttl time.Duration) (string, error) {
	output, err := a.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(a.key(objectName)),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("presign s3 object: %w", err)
	}
	return output.URL, nil
}

func (a *S3Adapter) deleteByPrefix(ctx context.Context, prefix string) error {
	paginator := s3.NewListObjectsV2Paginator(a.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(a.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list s3 objects: %w", err)
		}
		if len(page.Contents) == 0 {
			continue
		}

		objects := make([]types.ObjectIdentifier, 0, len(page.Contents))
		for _, object := range page.Contents {
			if object.Key == nil {
				continue
			}
			objects = append(objects, types.ObjectIdentifier{Key: object.Key})
		}
		if len(objects) == 0 {
			continue
		}

		output, err := a.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(a.bucket),
			Delete: &types.Delete{
				Objects: objects,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("delete s3 objects: %w", err)
		}
		if err := s3DeleteErrorsError(output.Errors); err != nil {
			return err
		}
	}

	return nil
}

func (a *S3Adapter) key(objectName string) string {
	return strings.TrimRight(a.keyPrefix, "/") + "/" + strings.TrimLeft(objectName, "/")
}

func (a *S3Adapter) clearPrefix() string {
	return strings.TrimRight(a.keyPrefix, "/") + "/"
}

func s3KeyPrefix(prefix string) string {
	if strings.TrimSpace(prefix) == "" {
		return defaultS3KeyPrefix
	}
	return strings.Trim(prefix, "/")
}

func s3DeleteErrorsError(deleteErrors []types.Error) error {
	if len(deleteErrors) == 0 {
		return nil
	}

	messages := make([]string, 0, len(deleteErrors))
	for _, deleteError := range deleteErrors {
		key := aws.ToString(deleteError.Key)
		code := aws.ToString(deleteError.Code)
		message := aws.ToString(deleteError.Message)

		parts := make([]string, 0, 3)
		if key != "" {
			parts = append(parts, "key="+key)
		}
		if code != "" {
			parts = append(parts, "code="+code)
		}
		if message != "" {
			parts = append(parts, "message="+message)
		}
		if len(parts) == 0 {
			messages = append(messages, "unknown object delete error")
			continue
		}
		messages = append(messages, strings.Join(parts, " "))
	}

	return fmt.Errorf("delete s3 objects reported %d object error(s): %s", len(deleteErrors), strings.Join(messages, "; "))
}

func isDirectS3FolderObject(prefix, key string) bool {
	if !strings.HasPrefix(key, prefix) {
		return false
	}
	name := strings.TrimPrefix(key, prefix)
	return name != "" && !strings.Contains(name, "/")
}

func isS3NotFound(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "NoSuchKey", "NotFound":
		return true
	default:
		return false
	}
}
