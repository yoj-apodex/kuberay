package s3

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/ray-project/kuberay/historyserver/pkg/collector/driverarchive"
)

func TestArchiveConditionalHeadersAreSignedAndErrorsMapped(t *testing.T) {
	count := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		if r.Header.Get("Authorization") == "" {
			t.Error("unsigned request")
		}
		if count == 1 && r.Header.Get("If-None-Match") != "*" {
			t.Error("missing create guard")
		}
		if count == 2 && r.Header.Get("If-Match") != "\"old\"" {
			t.Error("missing CAS guard")
		}
		if count == 2 {
			w.WriteHeader(412)
			return
		}
		w.Header().Set("ETag", "\"new\"")
		w.WriteHeader(200)
	}))
	defer server.Close()
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1"), Endpoint: aws.String(server.URL), S3ForcePathStyle: aws.Bool(true), Credentials: credentials.NewStaticCredentials("key", "secret", ""), MaxRetries: aws.Int(0)}))
	store := &RayLogsHandler{S3Client: awss3.New(sess), S3Bucket: "bucket"}
	tag, err := store.Put(context.Background(), "key", []byte("data"), "")
	if err != nil || tag != "\"new\"" {
		t.Fatalf("%s %v", tag, err)
	}
	_, err = store.Put(context.Background(), "key", []byte("new"), "\"old\"")
	if !errors.Is(err, driverarchive.ErrConflict) {
		t.Fatal(err)
	}
}
