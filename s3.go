package singleply

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/service/s3"
)

// type Connector interface {
// 	ListDir(path string, status StatusCallback) ([]*FileStat, error)
// 	PrepareForRead(path string, localPath string, offset uint64, length uint64, status StatusCallback) (prepared Region, err error)
// }

var BadLength error = errors.New("Bad length read")

func copyTo(localPath string, offset uint64, length uint64, reader io.ReadCloser) error {
	defer reader.Close()

	w, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer w.Close()

	_, err = w.Seek(int64(offset), 0)
	if err != nil {
		return err
	}

	written, err := io.Copy(w, reader)
	if err != nil {
		return err
	}

	if written != int64(length) {
		return BadLength
	}

	return nil
}

type S3Connection struct {
	bucket   string
	prefix   string
	region   string
	endpoint string
	svc      *s3.S3
}

func NewConnection(bucket string, prefix string, region string, endpoint string) *S3Connection {
	config := aws.NewConfig().WithCredentials(credentials.AnonymousCredentials).WithEndpoint(endpoint).WithRegion(region).WithS3ForcePathStyle(true)

	svc := s3.New(config)

	return &S3Connection{bucket: bucket, prefix: prefix, region: region, endpoint: endpoint, svc: svc}
}

func (c *S3Connection) PrepareForRead(path string, localPath string, offset uint64, length uint64, status StatusCallback) (prepared *Region, err error) {
	defaults.DefaultConfig.Region = aws.String("us-east-1")

	key := c.prefix + "/" + path
	input := s3.GetObjectInput{Bucket: &c.bucket,
		Key:   &key,
		Range: aws.String(fmt.Sprintf("%d-%d", offset, offset+length-1))}

	result, err := c.svc.GetObject(&input)
	err = copyTo(localPath, offset, length, result.Body)

	if err != nil {
		return nil, err
	}

	return &Region{offset, length}, err
}

func (c *S3Connection) ListDir(path string, status StatusCallback) ([]*FileStat, error) {
	files := make([]*FileStat, 0, 100)
	prefix := c.prefix + "/" + path
	input := s3.ListObjectsInput{Bucket: aws.String(c.bucket), Delimiter: aws.String("/"), Prefix: &prefix}

	err := c.svc.ListObjectsPages(&input, func(p *s3.ListObjectsOutput, lastPage bool) bool {
		for _, object := range p.Contents {
			name := (*object.Key)[len(prefix):]
			isDir := false
			files = append(files, &FileStat{Name: name, IsDir: isDir, Size: uint64(*object.Size)})
		}

		// do I need to do something to handle cases where there are objects with keys like "dir/".  It appears that they get returned in
		// ListObjectsPages.  Or maybe that's a bug in fakes3?

		for _, p := range p.CommonPrefixes {
			name := *p.Prefix
			name = name[len(prefix) : len(name)-1]
			files = append(files, &FileStat{Name: name, IsDir: true, Size: uint64(0)})
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}
