package singleply

import (
	"fmt"
	"os"
	"io/ioutil"

	"bytes"
	"log"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	storage "google.golang.org/api/storage/v1"
	. "gopkg.in/check.v1"
)

type GCSSuite struct {
}

var _ = Suite(&GCSSuite{})
var _ = fmt.Sprintf("hello!")

func insertDummyObj(service *storage.Service, bucket string, key string, length int) {
	object := &storage.Object{Name: key}
	buf := bytes.NewBuffer(make([]byte, length))
	if _, err := service.Objects.Insert(bucket, object).Media(buf).Do(); err == nil {
	} else {
		log.Panicf("Objects.Insert failed: %v", err)
	}

}

func (s *GCSSuite) TestGCSSuite(c *C) {
	bucket := "sply-test"
	prefix := "a"
	ctx := context.Background()

	// create google client to store sample file
	client, err := google.DefaultClient(context.Background())
	if err != nil {
		log.Panicf("Unable to get default client: %v", err)
	}
	service, err := storage.New(client)
	if err != nil {
		log.Panicf("Unable to create storage service: %v", err)
	}

	insertDummyObj(service, bucket, "a/sample", 1000)

	// Create GCS connection to test operations
	connection := NewGCSConnection(bucket, prefix)
	status := &NullStatusCallback{}
	files, err := connection.ListDir(ctx, "", status)
	c.Assert(len(files.Files) >= 1, Equals, true)

	var found *FileStat
	for _, x := range files.Files {
		if x.Name == "sample" {
			found = x
		}
	}
	c.Assert(found, NotNil)

	c.Assert(err, IsNil)

	localPath := c.MkDir() + "/dest"
	ioutil.WriteFile(localPath, make([]byte, 0), 0700)
	localPathWriter, err := os.OpenFile(localPath, os.O_RDWR, 0)
	c.Assert(err, IsNil)
	defer localPathWriter.Close()
	
	region, err := connection.PrepareForRead(ctx, "sample", found.Etag, localPathWriter, 0, 10, status)
	c.Assert(err, IsNil)
	c.Assert(region.Offset, Equals, uint64(0))
	c.Assert(region.Length, Equals, uint64(10))

	insertDummyObj(service, bucket, "a/sample", 1010)

	// this should generate an error because the file is different than when we started reading
	region, err = connection.PrepareForRead(ctx, "sample", found.Etag, localPathWriter, 990, 10, status)
	c.Assert(err, Equals, UpdateDetected)
}
