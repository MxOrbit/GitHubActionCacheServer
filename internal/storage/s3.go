package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const (
	s3MinimumComposePartSize int64 = 5 * 1024 * 1024
	s3MaximumComposePartSize int64 = 5 * 1024 * 1024 * 1024
	s3MaximumComposeParts          = 10_000
)

type s3ComposePart struct {
	sourceObjectName string
	start            int64
	size             int64
	sourceSize       int64
}

type S3Adapter struct {
	client    *s3.Client
	presign   *s3.PresignClient
	transfer  *transfermanager.Client
	bucket    string
	keyPrefix string

	uploadPartSizeBytes   int64
	uploadConcurrency     int
	multipartAbortTimeout time.Duration
}

func NewS3Adapter(ctx context.Context, cfg config.StorageConfig) (*S3Adapter, error) {
	if cfg.S3Bucket == "" {
		return nil, fmt.Errorf("STORAGE_S3_BUCKET is required")
	}
	uploadPartSizeBytes, err := normalizeS3UploadPartSizeBytes(cfg.S3UploadPartSizeBytes, cfg.S3UploadPartSizeBytesConfigured)
	if err != nil {
		return nil, err
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

	keyPrefix := s3KeyPrefix(cfg.S3KeyPrefix)
	uploadConcurrency := s3UploadConcurrency(cfg.S3UploadConcurrency)
	multipartAbortTimeout := s3MultipartAbortTimeout(cfg.S3MultipartAbortTimeout)
	if _, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(cfg.S3Bucket),
		Prefix:  aws.String(keyPrefix + "/"),
		MaxKeys: aws.Int32(1),
	}); err != nil {
		if isS3BucketNotFound(err) {
			return nil, fmt.Errorf("bucket %q does not exist", cfg.S3Bucket)
		}
		return nil, fmt.Errorf("probe s3 bucket prefix: %w", err)
	}

	return &S3Adapter{
		client:                client,
		presign:               s3.NewPresignClient(client),
		transfer:              newS3TransferManager(client, uploadPartSizeBytes, uploadConcurrency),
		bucket:                cfg.S3Bucket,
		keyPrefix:             keyPrefix,
		uploadPartSizeBytes:   uploadPartSizeBytes,
		uploadConcurrency:     uploadConcurrency,
		multipartAbortTimeout: multipartAbortTimeout,
	}, nil
}

func (a *S3Adapter) UploadStream(ctx context.Context, objectName string, stream io.Reader) error {
	transfer := a.transfer
	if transfer == nil {
		transfer = newS3TransferManager(a.client, s3UploadPartSizeBytes(a.uploadPartSizeBytes), s3UploadConcurrency(a.uploadConcurrency))
	}
	_, err := transfer.UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(a.key(objectName)),
		Body:   stream,
	})
	if err != nil {
		if abortErr := a.abortFailedMultipartUpload(objectName, err); abortErr != nil {
			return fmt.Errorf("upload s3 object: %w; %v", err, abortErr)
		}
		return fmt.Errorf("upload s3 object: %w", err)
	}
	return nil
}

func (a *S3Adapter) CopyObject(ctx context.Context, sourceObjectName, destinationObjectName string) error {
	_, err := a.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(a.bucket),
		CopySource: aws.String(url.PathEscape(a.bucket + "/" + a.key(sourceObjectName))),
		Key:        aws.String(a.key(destinationObjectName)),
	})
	if err != nil {
		if isS3NotFound(err) {
			return ObjectNotFoundError{ObjectName: sourceObjectName}
		}
		return fmt.Errorf("copy s3 object: %w", err)
	}
	return nil
}

func (a *S3Adapter) ComposeObjects(ctx context.Context, destinationObjectName string, sourceObjectNames []string) error {
	if len(sourceObjectNames) == 0 {
		return fmt.Errorf("%w: no source objects", ErrComposeUnsupported)
	}
	if len(sourceObjectNames) > s3MaximumComposeParts {
		return fmt.Errorf("%w: %d sources exceed the S3 limit of %d parts", ErrComposeUnsupported, len(sourceObjectNames), s3MaximumComposeParts)
	}

	sourceSizes := make([]int64, len(sourceObjectNames))
	for index, sourceObjectName := range sourceObjectNames {
		output, err := a.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(a.bucket),
			Key:    aws.String(a.key(sourceObjectName)),
		})
		if err != nil {
			if isS3NotFound(err) {
				return ObjectNotFoundError{ObjectName: sourceObjectName}
			}
			if isS3ComposeUnsupported(err) {
				return fmt.Errorf("%w: inspect compose source: %v", ErrComposeUnsupported, err)
			}
			return fmt.Errorf("inspect s3 compose source: %w", err)
		}
		sourceSizes[index] = aws.ToInt64(output.ContentLength)
	}

	parts, err := planS3ComposeParts(sourceObjectNames, sourceSizes)
	if err != nil {
		return err
	}

	created, err := a.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(a.bucket),
		Key:    aws.String(a.key(destinationObjectName)),
	})
	if err != nil {
		if isS3ComposeUnsupported(err) {
			return fmt.Errorf("%w: create multipart upload: %v", ErrComposeUnsupported, err)
		}
		return fmt.Errorf("create s3 compose upload: %w", err)
	}
	uploadID := aws.ToString(created.UploadId)
	if uploadID == "" {
		return fmt.Errorf("create s3 compose upload: missing upload id")
	}

	completedParts := make([]types.CompletedPart, 0, len(parts))
	for index, part := range parts {
		partNumber := int32(index + 1)
		input := &s3.UploadPartCopyInput{
			Bucket:     aws.String(a.bucket),
			CopySource: aws.String(url.PathEscape(a.bucket + "/" + a.key(part.sourceObjectName))),
			Key:        aws.String(a.key(destinationObjectName)),
			PartNumber: aws.Int32(partNumber),
			UploadId:   aws.String(uploadID),
		}
		if part.start != 0 || part.size != part.sourceSize {
			input.CopySourceRange = aws.String(fmt.Sprintf("bytes=%d-%d", part.start, part.start+part.size-1))
		}

		copied, copyErr := a.client.UploadPartCopy(ctx, input)
		if copyErr != nil {
			return a.failS3Compose(destinationObjectName, uploadID, fmt.Errorf("copy s3 compose part %d: %w", partNumber, copyErr))
		}
		if copied.CopyPartResult == nil || copied.CopyPartResult.ETag == nil {
			return a.failS3Compose(destinationObjectName, uploadID, fmt.Errorf("copy s3 compose part %d: missing etag", partNumber))
		}
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       copied.CopyPartResult.ETag,
			PartNumber: aws.Int32(partNumber),
		})
	}

	_, err = a.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(a.bucket),
		Key:      aws.String(a.key(destinationObjectName)),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return a.failS3Compose(destinationObjectName, uploadID, fmt.Errorf("complete s3 compose upload: %w", err))
	}
	return nil
}

func planS3ComposeParts(sourceObjectNames []string, sourceSizes []int64) ([]s3ComposePart, error) {
	if len(sourceObjectNames) == 0 || len(sourceObjectNames) != len(sourceSizes) {
		return nil, fmt.Errorf("%w: invalid source metadata", ErrComposeUnsupported)
	}

	parts := make([]s3ComposePart, 0, len(sourceObjectNames))
	for index, sourceObjectName := range sourceObjectNames {
		sourceSize := sourceSizes[index]
		if sourceSize <= 0 {
			return nil, fmt.Errorf("%w: source %d is empty", ErrComposeUnsupported, index)
		}

		partCount := int((sourceSize + s3MaximumComposePartSize - 1) / s3MaximumComposePartSize)
		baseSize := sourceSize / int64(partCount)
		extraBytes := sourceSize % int64(partCount)
		start := int64(0)
		for partIndex := 0; partIndex < partCount; partIndex++ {
			size := baseSize
			if int64(partIndex) < extraBytes {
				size++
			}
			parts = append(parts, s3ComposePart{
				sourceObjectName: sourceObjectName,
				start:            start,
				size:             size,
				sourceSize:       sourceSize,
			})
			start += size
		}
	}

	if len(parts) > s3MaximumComposeParts {
		return nil, fmt.Errorf("%w: %d parts exceed the S3 limit of %d", ErrComposeUnsupported, len(parts), s3MaximumComposeParts)
	}
	for index, part := range parts[:len(parts)-1] {
		if part.size < s3MinimumComposePartSize {
			return nil, fmt.Errorf("%w: non-final part %d is smaller than %d bytes", ErrComposeUnsupported, index, s3MinimumComposePartSize)
		}
	}
	return parts, nil
}

func (a *S3Adapter) failS3Compose(destinationObjectName, uploadID string, composeErr error) error {
	if isS3ComposeUnsupported(composeErr) {
		composeErr = fmt.Errorf("%w: %v", ErrComposeUnsupported, composeErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s3MultipartAbortTimeout(a.multipartAbortTimeout))
	defer cancel()

	_, abortErr := a.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(a.bucket),
		Key:      aws.String(a.key(destinationObjectName)),
		UploadId: aws.String(uploadID),
	})
	if abortErr != nil && !isS3NotFound(abortErr) {
		return fmt.Errorf("%w; abort s3 compose upload: %v", composeErr, abortErr)
	}
	return composeErr
}

func (a *S3Adapter) abortFailedMultipartUpload(objectName string, uploadErr error) error {
	var multiErr transfermanager.MultipartUploadError
	if !errors.As(uploadErr, &multiErr) || multiErr.UploadID() == "" {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), s3MultipartAbortTimeout(a.multipartAbortTimeout))
	defer cancel()

	_, err := a.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(a.bucket),
		Key:      aws.String(a.key(objectName)),
		UploadId: aws.String(multiErr.UploadID()),
	})
	if err != nil && !isS3NotFound(err) {
		return fmt.Errorf("abort s3 multipart upload: %w", err)
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

func newS3TransferManager(client transfermanager.S3APIClient, partSizeBytes int64, concurrency int) *transfermanager.Client {
	return transfermanager.New(client, func(options *transfermanager.Options) {
		options.PartSizeBytes = partSizeBytes
		options.MultipartUploadThreshold = partSizeBytes
		options.Concurrency = concurrency
	})
}

func s3KeyPrefix(prefix string) string {
	if strings.TrimSpace(prefix) == "" {
		return config.DefaultS3KeyPrefix
	}
	return strings.Trim(prefix, "/")
}

func s3UploadPartSizeBytes(partSizeBytes int64) int64 {
	if partSizeBytes < 1 {
		return config.DefaultS3UploadPartSizeBytes
	}
	return partSizeBytes
}

func normalizeS3UploadPartSizeBytes(partSizeBytes int64, configured bool) (int64, error) {
	if partSizeBytes == 0 && !configured {
		return config.DefaultS3UploadPartSizeBytes, nil
	}
	if partSizeBytes < config.MinS3UploadPartSizeBytes {
		return 0, fmt.Errorf("STORAGE_S3_UPLOAD_PART_SIZE_BYTES must be at least %d bytes (5 MiB), got %d", config.MinS3UploadPartSizeBytes, partSizeBytes)
	}
	return partSizeBytes, nil
}

func s3UploadConcurrency(concurrency int) int {
	if concurrency < 1 {
		return config.DefaultS3UploadConcurrency
	}
	return concurrency
}

func s3MultipartAbortTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		return config.DefaultS3MultipartAbortTimeout
	}
	return timeout
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
	return isS3ErrorCode(err, "NoSuchKey", "NoSuchUpload", "NotFound")
}

func isS3BucketNotFound(err error) bool {
	return isS3ErrorCode(err, "NoSuchBucket", "NotFound")
}

func isS3ComposeUnsupported(err error) bool {
	return errors.Is(err, ErrComposeUnsupported) || isS3ErrorCode(err, "EntityTooSmall", "NotImplemented", "NotImplementedException")
}

func isS3ErrorCode(err error, codes ...string) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	for _, code := range codes {
		if apiErr.ErrorCode() == code {
			return true
		}
	}
	return false
}
