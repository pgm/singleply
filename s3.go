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

	w, err := os.OpenFile(localPath, os.O_RDWR, 0777);
	if err != nil {
		return err
	}
	//fmt.Printf("copyTo(%s,%d,%d,%s)\n", localPath, offset, length, reader)
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
		return errors.New(fmt.Sprintf("Expected to write %d bytes, but wrote %d", length, written))
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

func NewS3Connection(creds *credentials.Credentials, bucket string, prefix string, region string, endpoint string) *S3Connection {
	config := aws.NewConfig().WithCredentials(creds).WithEndpoint(endpoint).WithRegion(region).WithS3ForcePathStyle(true)

	svc := s3.New(config)

	return &S3Connection{bucket: bucket, prefix: prefix, region: region, endpoint: endpoint, svc: svc}
}

func (c *S3Connection) PrepareForRead(path string, etag string, localPath string, offset uint64, length uint64, status StatusCallback) (prepared *Region, err error) {
	defaults.DefaultConfig.Region = aws.String("us-east-1")

	key := c.prefix + "/" + path
	input := s3.GetObjectInput{Bucket: &c.bucket,
		IfMatch: &etag,
		Key:   &key,
		Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))}

	result, err := c.svc.GetObject(&input)
	err = copyTo(localPath, offset, length, result.Body)

	if err != nil {
		return nil, err
	}

	return &Region{offset, length}, err
}

func (c *S3Connection) ListDir(path string, status StatusCallback) (*DirEntries, error) {
	files := make([]*FileStat, 0, 100)
	if path != "" {
		path = path + "/"
	}
	prefix := c.prefix + "/" + path
	fmt.Printf("ListDir(prefix=\"%s\")\n", prefix)
	input := s3.ListObjectsInput{Bucket: aws.String(c.bucket), Delimiter: aws.String("/"), Prefix: &prefix}

	// Handle cases where there are objects with keys like "dir/".  "dir" will be both a key and a common prefix
	// Not sure if this is a bug in fakes3 though, because there should be no key with the name "dir".  However,
	// filtering to avoid issues with both key and directry with same name
	dirNames := make(map[string]string)

	err := c.svc.ListObjectsPages(&input, func(p *s3.ListObjectsOutput, lastPage bool) bool {
		fmt.Printf("ListObjectPages returned %s\n", p)
		
		for _, p := range p.CommonPrefixes {
			name := *p.Prefix
			name = name[len(prefix) : len(name)-1]

			fmt.Printf("Adding dir \"%s\" for prefix %s\n", name, (*p.Prefix))
			files = append(files, &FileStat{Name: name, IsDir: true, Size: uint64(0)})
			dirNames[name] = name
		}

		for _, object := range p.Contents {
			name := (*object.Key)[len(prefix):]

			if _, present := dirNames[name]; present {
				fmt.Printf("Skipping file %s for key %s because a dir with that name exists\n", name, (*object.Key))
				continue
			}

			fmt.Printf("Adding file \"%s\" for key \"%s\"\n", name, (*object.Key))
			isDir := false

			if name == "" {
				//name = "INVALID"
				continue
			}

			files = append(files, &FileStat{Name: name, IsDir: isDir, Size: uint64(*object.Size), Etag: *object.ETag})
		}

		return true
	})

	if err != nil {
		return nil, err
	}

	return &DirEntries{Files: files}, nil
}
