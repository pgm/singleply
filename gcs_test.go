package singleply

 import (
//         "flag"
         "fmt"
//         "log"

		 "io/ioutil"
//         "golang.org/x/net/context"
//         "golang.org/x/oauth2/google"
//         storage "google.golang.org/api/storage/v1"
	
        "golang.org/x/net/context"
        "golang.org/x/oauth2/google"
        storage "google.golang.org/api/storage/v1"
		"log"
		"bytes"
	. "gopkg.in/check.v1"
)

type GCSSuite struct {
	
}

var _ = Suite(&GCSSuite{})
var _ = fmt.Sprintf("hello!")

func (s *GCSSuite) TestGCSSuite(c *C) {		
	bucket := "sply-test"
	prefix := "a"

	// create google client to store sample file	
	client, err := google.DefaultClient(context.Background())
	if err != nil {
			log.Fatalf("Unable to get default client: %v", err)
	}
	service, err := storage.New(client)
	if err != nil {
			log.Fatalf("Unable to create storage service: %v", err)
	}
	object := &storage.Object{Name: "a/sample"}	
	buf := bytes.NewBuffer(make([]byte,1000))
	if res, err := service.Objects.Insert(bucket, object).Media(buf).Do(); err == nil {
			fmt.Printf("Created object %v at location %v\n\n", res.Name, res.SelfLink)
	} else {
			log.Fatalf("Objects.Insert failed: %v", err)
	}	

	// Create GCS connection to test operations	
	connection := NewGCSConnection(bucket, prefix)
	status := &NullStatusCallback{}
	files, err := connection.ListDir("", status)
	c.Assert(len(files.Files) >= 1, Equals, true)
	
	var found *FileStat
	for _, x := range files.Files {
		if x.Name == "sample" {
			found = x
		}
	}
	c.Assert(found, NotNil)
	
	c.Assert(err, IsNil)

	localPath := c.MkDir()+"/dest"
	ioutil.WriteFile(localPath, make([]byte, 0), 0700)	
	
	region, err := connection.PrepareForRead("sample", found.Etag, localPath, 0, 10, status)
	c.Assert(err, IsNil)
	c.Assert(region.Offset, Equals, uint64(0))
	c.Assert(region.Length, Equals, uint64(10))
	fmt.Printf("files=%s\n", files)
}



