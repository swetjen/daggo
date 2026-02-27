package s3resource

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"reflect"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
)

type NewClientFunc func(endpoint string, opts *minio.Options) (*minio.Client, error)

type Resource struct {
	BucketName            string
	EntitiesBucket        string
	RequireEntitiesBucket bool
	S3URL                 string
	Region                string
	AccessKey             string
	SecretKey             string
	UseSSL                bool
	ServerSideEncryption  string
	SSEKMSKeyID           string

	NewClient NewClientFunc
}

type BatchUploadItem struct {
	Data     any
	BlobPath string
}

func NewFromEnv() *Resource {
	s3URL := envOr("S3_URL", "http://localhost:9000")
	return &Resource{
		BucketName:            envOr("S3_BUCKET_NAME", "daggo-artifacts"),
		EntitiesBucket:        strings.TrimSpace(os.Getenv("S3_ENTITIES_BUCKET_NAME")),
		RequireEntitiesBucket: envBool("S3_ENTITIES_BUCKET_REQUIRED", false),
		S3URL:                 s3URL,
		Region:                envOr("S3_REGION", "us-east-1"),
		AccessKey:             envOr("S3_ACCESS_KEY_ID", "dummy"),
		SecretKey:             envOr("S3_SECRET_ACCESS_KEY", "dummy"),
		UseSSL:                strings.EqualFold(strings.TrimSpace(strings.SplitN(s3URL, "://", 2)[0]), "https"),
		ServerSideEncryption:  strings.TrimSpace(os.Getenv("S3_SERVER_SIDE_ENCRYPTION")),
		SSEKMSKeyID:           strings.TrimSpace(os.Getenv("S3_SSE_KMS_KEY_ID")),
		NewClient:             minio.New,
	}
}

func (r *Resource) GetClient() (*minio.Client, error) {
	if r == nil {
		return nil, fmt.Errorf("s3resource: resource is nil")
	}
	parsed, err := url.Parse(strings.TrimSpace(r.S3URL))
	if err != nil {
		return nil, fmt.Errorf("s3resource: parse S3 URL: %w", err)
	}
	endpoint := parsed.Host
	if endpoint == "" {
		endpoint = parsed.Path
	}
	if strings.TrimSpace(endpoint) == "" {
		return nil, fmt.Errorf("s3resource: invalid S3 URL %q", r.S3URL)
	}

	secure := r.UseSSL
	if parsed.Scheme != "" {
		secure = strings.EqualFold(parsed.Scheme, "https")
	}

	newClient := r.NewClient
	if newClient == nil {
		newClient = minio.New
	}

	client, err := newClient(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(r.AccessKey, r.SecretKey, ""),
		Secure:       secure,
		Region:       r.Region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, fmt.Errorf("s3resource: create client: %w", err)
	}
	return client, nil
}

func (r *Resource) UploadData(ctx context.Context, data any, blobPath string) (string, error) {
	client, err := r.GetClient()
	if err != nil {
		return "", err
	}
	bucket := r.bucketName()
	if err := r.ensureBucket(ctx, client, bucket); err != nil {
		return "", err
	}
	body, err := ConvertToUploadBody(data)
	if err != nil {
		return "", err
	}
	contentType := InferContentType(data)
	putOptions, err := r.putObjectOptions(contentType)
	if err != nil {
		return "", err
	}
	_, err = client.PutObject(ctx, bucket, blobPath, bytes.NewReader(body), int64(len(body)), putOptions)
	if err != nil {
		return "", fmt.Errorf("s3resource: upload %s: %w", blobPath, err)
	}
	return fmt.Sprintf("s3://%s/%s", bucket, blobPath), nil
}

func (r *Resource) DownloadData(ctx context.Context, blobPath string) ([]byte, error) {
	client, err := r.GetClient()
	if err != nil {
		return nil, err
	}
	obj, err := client.GetObject(ctx, r.bucketName(), blobPath, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("s3resource: get object %s: %w", blobPath, err)
	}
	defer func() {
		_ = obj.Close()
	}()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("s3resource: read object %s: %w", blobPath, err)
	}
	return data, nil
}

func (r *Resource) BlobExists(ctx context.Context, blobPath string) bool {
	client, err := r.GetClient()
	if err != nil {
		return false
	}
	_, err = client.StatObject(ctx, r.bucketName(), blobPath, minio.StatObjectOptions{})
	return err == nil
}

func (r *Resource) GetPublicURL(blobPath string) string {
	base := strings.TrimRight(strings.TrimSpace(r.S3URL), "/")
	return fmt.Sprintf("%s/%s/%s", base, r.bucketName(), blobPath)
}

func (r *Resource) BatchUploadEntityData(ctx context.Context, batchData []BatchUploadItem) ([]string, error) {
	client, err := r.GetClient()
	if err != nil {
		return nil, err
	}
	bucket, err := r.entitiesBucket()
	if err != nil {
		return nil, err
	}
	if err := r.ensureBucket(ctx, client, bucket); err != nil {
		return nil, err
	}

	results := make([]string, 0, len(batchData))
	for _, item := range batchData {
		body, err := ConvertToUploadBody(item.Data)
		if err != nil {
			return nil, err
		}
		contentType := InferContentType(item.Data)
		putOptions, err := r.putObjectOptions(contentType)
		if err != nil {
			return nil, err
		}
		_, err = client.PutObject(ctx, bucket, item.BlobPath, bytes.NewReader(body), int64(len(body)), putOptions)
		if err != nil {
			return nil, fmt.Errorf("s3resource: batch upload %s: %w", item.BlobPath, err)
		}
		results = append(results, fmt.Sprintf("s3://%s/%s", bucket, item.BlobPath))
	}

	return results, nil
}

func ConvertToUploadBody(data any) ([]byte, error) {
	switch typed := data.(type) {
	case nil:
		return []byte("null"), nil
	case []byte:
		return typed, nil
	case string:
		return []byte(typed), nil
	}

	value := reflect.ValueOf(data)
	switch value.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array, reflect.Struct:
		payload, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("s3resource: marshal payload: %w", err)
		}
		return payload, nil
	default:
		return []byte(fmt.Sprint(data)), nil
	}
}

func InferContentType(data any) string {
	switch data.(type) {
	case string:
		return "text/plain"
	case []byte:
		return "application/octet-stream"
	}

	value := reflect.ValueOf(data)
	switch value.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array, reflect.Struct:
		return "application/json"
	default:
		return "application/octet-stream"
	}
}

func (r *Resource) ensureBucket(ctx context.Context, client *minio.Client, bucket string) error {
	exists, err := client.BucketExists(ctx, bucket)
	if err == nil && exists {
		return nil
	}

	makeErr := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: r.Region})
	if makeErr == nil {
		return nil
	}

	exists, existsErr := client.BucketExists(ctx, bucket)
	if existsErr == nil && exists {
		return nil
	}

	if err != nil {
		return fmt.Errorf("s3resource: ensure bucket %s after probe failure: %w", bucket, err)
	}
	return fmt.Errorf("s3resource: ensure bucket %s: %w", bucket, makeErr)
}

func (r *Resource) putObjectOptions(contentType string) (minio.PutObjectOptions, error) {
	options := minio.PutObjectOptions{ContentType: contentType}
	mode := strings.ToUpper(strings.TrimSpace(r.ServerSideEncryption))
	switch mode {
	case "":
		return options, nil
	case "SSE-S3", "AES256":
		options.ServerSideEncryption = encrypt.NewSSE()
		return options, nil
	case "SSE-KMS", "AWS:KMS", "KMS":
		keyID := strings.TrimSpace(r.SSEKMSKeyID)
		if keyID == "" {
			return minio.PutObjectOptions{}, errors.New("s3resource: S3_SSE_KMS_KEY_ID is required for SSE-KMS")
		}
		sse, err := encrypt.NewSSEKMS(keyID, nil)
		if err != nil {
			return minio.PutObjectOptions{}, fmt.Errorf("s3resource: configure SSE-KMS: %w", err)
		}
		options.ServerSideEncryption = sse
		return options, nil
	default:
		return minio.PutObjectOptions{}, fmt.Errorf("s3resource: unsupported S3_SERVER_SIDE_ENCRYPTION mode %q", r.ServerSideEncryption)
	}
}

func (r *Resource) bucketName() string {
	if r == nil {
		return "daggo-artifacts"
	}
	name := strings.TrimSpace(r.BucketName)
	if name == "" {
		name = "daggo-artifacts"
	}
	return name
}

func (r *Resource) entitiesBucket() (string, error) {
	if r == nil {
		return "daggo-artifacts", nil
	}
	name := strings.TrimSpace(r.EntitiesBucket)
	if name != "" {
		return name, nil
	}
	if r.RequireEntitiesBucket {
		return "", errors.New("s3resource: entities bucket is required (set S3_ENTITIES_BUCKET_NAME)")
	}
	return r.bucketName(), nil
}

func envOr(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func envBool(name string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	switch value {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}
