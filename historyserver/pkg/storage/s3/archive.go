package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/ray-project/kuberay/historyserver/pkg/collector/driverarchive"
)

func archiveError(err error) error {
	var e awserr.RequestFailure
	if errors.As(err, &e) {
		switch e.StatusCode() {
		case 404:
			return driverarchive.ErrNotFound
		case 409, 412:
			return driverarchive.ErrConflict
		}
	}
	return err
}
func (r *RayLogsHandler) Get(ctx context.Context, key string, limit int64) ([]byte, string, error) {
	obj, err := r.S3Client.GetObjectWithContext(ctx, &awss3.GetObjectInput{Bucket: aws.String(r.S3Bucket), Key: aws.String(key)})
	if err != nil {
		return nil, "", archiveError(err)
	}
	defer obj.Body.Close()
	if aws.Int64Value(obj.ContentLength) > limit {
		return nil, "", fmt.Errorf("archive object exceeds %d bytes", limit)
	}
	body, err := io.ReadAll(io.LimitReader(obj.Body, limit+1))
	if err != nil {
		return nil, "", err
	}
	if int64(len(body)) > limit {
		return nil, "", errors.New("archive object exceeds limit")
	}
	return body, aws.StringValue(obj.ETag), nil
}
func (r *RayLogsHandler) Put(ctx context.Context, key string, body []byte, expected string) (string, error) {
	req, out := r.S3Client.PutObjectRequest(&awss3.PutObjectInput{Bucket: aws.String(r.S3Bucket), Key: aws.String(key), Body: bytes.NewReader(body)})
	req.SetContext(ctx)
	// aws-sdk-go v1 predates these modeled fields. Headers are attached before
	// Build/Sign and retained on retries; S3 enforces them, not a local mutex.
	if expected == "" {
		req.HTTPRequest.Header.Set("If-None-Match", "*")
	} else {
		req.HTTPRequest.Header.Set("If-Match", expected)
	}
	if err := req.Send(); err != nil {
		return "", archiveError(err)
	}
	return aws.StringValue(out.ETag), nil
}
