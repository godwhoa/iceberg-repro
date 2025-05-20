package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	_ "github.com/apache/iceberg-go/catalog/rest"
)

func main() {
	props := iceberg.Properties{
		"uri":                  "http://localhost:8181",
		"type":                 "rest",
		"s3.endpoint":          "http://localhost:9000",
		"s3.access-key-id":     "admin",
		"s3.secret-access-key": "password",
		"s3.path-style-access": "true",
		"s3.use-ssl":           "false",
	}

	ctx := context.TODO()

	cat, err := catalog.Load(ctx, "demo", props)
	if err != nil {
		panic(err)
	}

	cat.CreateNamespace(ctx, []string{"test"}, iceberg.Properties{})

	partSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID:  1020,
			FieldID:   1,
			Transform: iceberg.DayTransform{},
			Name:      "date_partition",
		},
	)

	_, err = cat.CreateTable(ctx, []string{"test", "ecs"}, iceberg.NewSchema(0,
		iceberg.NestedField{
			ID: 1000, Name: "id", Type: iceberg.PrimitiveTypes.String, Required: true,
		},
		iceberg.NestedField{
			ID: 1010, Name: "identity", Type: iceberg.PrimitiveTypes.String, Required: false,
		},
		iceberg.NestedField{
			ID: 1020, Name: "@timestamp", Type: iceberg.PrimitiveTypes.Timestamp, Required: true,
		},
	), catalog.WithPartitionSpec(&partSpec))
	if err != nil && !strings.Contains(err.Error(), "AlreadyExistsException") {
		panic(err)
	}

	var tbl *table.Table
	tbl, err = cat.LoadTable(ctx, []string{"test", "ecs"}, iceberg.Properties{})
	if err != nil {
		panic(err)
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("admin", "password", "")),
		config.WithEndpointResolver(aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{URL: "http://localhost:9000", SigningRegion: "us-east-1"}, nil
		})),
	)
	if err != nil {
		panic(err)
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	uploader := manager.NewUploader(s3Client)

	if err := UploadAndRegister(ctx, tbl, uploader, "warehouse", "test/ecs/data", "sample.parquet", "2024-12-03"); err != nil {
		panic(err)
	}

}

func UploadAndRegister(ctx context.Context, tbl *table.Table, uploader *manager.Uploader, bucket, prefix, localFilePath, partitionDay string) error {
	f, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("open parquet file: %w", err)
	}
	defer f.Close()

	key := filepath.ToSlash(filepath.Join(prefix, fmt.Sprintf("date_partition=%s", partitionDay), uuid.New().String()+".parquet"))
	if _, err := uploader.Upload(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: f}); err != nil {
		return fmt.Errorf("failed to upload to s3: %w", err)
	}
	fmt.Printf("Uploading to bucket: %s key: %s\n", bucket, key)

	txn := tbl.NewTransaction()
	s3URI := fmt.Sprintf("s3://%s/%s", bucket, key)
	if err := txn.AddFiles(ctx, []string{s3URI}, iceberg.Properties{}, false); err != nil {
		return fmt.Errorf("failed to add file to iceberg: %w", err)
	}

	_, err = txn.Commit(ctx)
	if err == nil {
		fmt.Printf("Added %s to iceberg table and commited.\n", s3URI)
	}
	return err
}
