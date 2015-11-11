package singleply

import (
	"bytes"
	"fmt"
	"time"

	"os/exec"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"

	. "gopkg.in/check.v1"
)

type S3Suite struct {
	fakes3Cmd *exec.Cmd
	svc       *s3.S3
	endpoint  string
	region    string
}

var _ = Suite(&S3Suite{})
var _ = fmt.Sprintf("hello!")

const S3Port = "4444"

func (s *S3Suite) SetUpSuite(c *C) {
	tmpdir := c.MkDir()
	exe, err := exec.LookPath("fakes3")
	if err != nil {
		panic(err.Error())
	}
	s.fakes3Cmd = exec.Command(exe, "--root", tmpdir, "--port", S3Port)
	err = s.fakes3Cmd.Start()
	if err != nil {
		panic(err.Error())
	}

	// wait until fakes3 is responding.  Give it 5 seconds before aborting

	timeout := time.Now().Add(time.Second * 5)

	s.endpoint = "http://localhost:" + S3Port
	s.region = "us-east-1"
	config := aws.NewConfig().WithCredentials(credentials.AnonymousCredentials).WithEndpoint(s.endpoint).WithRegion(s.region).WithS3ForcePathStyle(true)
	s.svc = s3.New(config)
	for {
		input := s3.ListBucketsInput{}
		_, err := s.svc.ListBuckets(&input)

		if err == nil {
			break
		}

		now := time.Now()
		if now.After(timeout) {
			panic("Timeout waiting for fakes3")
		}
	}
}

func (s *S3Suite) TearDownSuite(c *C) {
	s.fakes3Cmd.Process.Kill()
	s.fakes3Cmd.Wait()
}

// func (s *S3Suite) TestListObjects(c *C) {
// 	createBucket := s3.CreateBucketInput{Bucket: aws.String("TestListObjects")}
// 	_, err := s.svc.CreateBucket(&createBucket)
// 	c.Assert(err, IsNil)
//
// 	buffer := bytes.NewReader(make([]byte, 0))
// 	putObject := s3.PutObjectInput{}
// 	putObject.Bucket = aws.String("TestListObjects")
// 	putObject.Key = aws.String("prefix/dir/")
// 	putObject.Body = buffer
// 	s.svc.PutObject(&putObject)
//
// 	putObject = s3.PutObjectInput{}
// 	putObject.Bucket = aws.String("TestListObjects")
// 	putObject.Key = aws.String("prefix/dir/a")
// 	putObject.Body = buffer
// 	s.svc.PutObject(&putObject)
//
// 	putObject = s3.PutObjectInput{}
// 	putObject.Bucket = aws.String("TestListObjects")
// 	putObject.Key = aws.String("prefix/dir/b")
// 	putObject.Body = buffer
// 	s.svc.PutObject(&putObject)
//
// 	putObject = s3.PutObjectInput{}
// 	putObject.Bucket = aws.String("TestListObjects")
// 	putObject.Key = aws.String("prefix/c")
// 	putObject.Body = buffer
// 	s.svc.PutObject(&putObject)
//
// 	input := s3.ListObjectsInput{Bucket: aws.String("TestListObjects"), Delimiter: aws.String("/"), Prefix: aws.String("prefix/")}
//
// 	objs, err := s.svc.ListObjects(&input)
// 	fmt.Printf("contents: %s\n", objs.Contents)
// 	c.Assert(len(objs.CommonPrefixes), Equals, 1)
// 	c.Assert(len(objs.Contents), Equals, 0)
// }

func (s *S3Suite) TestConnection(c *C) {
	createBucket := s3.CreateBucketInput{Bucket: aws.String("bucket")}
	_, err := s.svc.CreateBucket(&createBucket)
	c.Assert(err, IsNil)

	conn := NewS3Connection("bucket", "prefix", s.region, s.endpoint)
	files, err := conn.ListDir("path", nil)
	c.Assert(err, IsNil)
	c.Assert(len(files.Files), Equals, 0)

	buffer := bytes.NewReader(make([]byte, 100))
	putObject := s3.PutObjectInput{}
	putObject.Bucket = aws.String("bucket")
	putObject.Key = aws.String("prefix/banana")
	putObject.Body = buffer
	s.svc.PutObject(&putObject)

	putObject = s3.PutObjectInput{}
	putObject.Bucket = aws.String("bucket")
	putObject.Key = aws.String("prefix/sampledir/")
	putObject.Body = bytes.NewReader(make([]byte, 0))
	s.svc.PutObject(&putObject)

	putObject = s3.PutObjectInput{}
	putObject.Bucket = aws.String("bucket")
	putObject.Key = aws.String("prefix/sampledir/a")
	putObject.Body = bytes.NewReader(make([]byte, 0))
	s.svc.PutObject(&putObject)

	putObject = s3.PutObjectInput{}
	putObject.Bucket = aws.String("bucket")
	putObject.Key = aws.String("prefix/sampledir/b")
	putObject.Body = bytes.NewReader(make([]byte, 0))
	s.svc.PutObject(&putObject)

	files, err = conn.ListDir("", nil)
	fmt.Printf("files=%s\n", files)
	c.Assert(err, IsNil)
	c.Assert(len(files.Files), Equals, 2)
	f := files.Files[1]
	c.Assert(f.Name, Equals, "banana")
	c.Assert(f.Size, Equals, uint64(100))
	c.Assert(f.IsDir, Equals, false)

	f = files.Files[0]
	c.Assert(f.Name, Equals, "sampledir")
	c.Assert(f.Size, Equals, uint64(0))
	c.Assert(f.IsDir, Equals, true)
}
