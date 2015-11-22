package singleply

 import (
//         "flag"
         "fmt"
//         "log"

		 "io/ioutil"
//         "golang.org/x/net/context"
//         "golang.org/x/oauth2/google"
//         storage "google.golang.org/api/storage/v1"
	. "gopkg.in/check.v1"
)

type GCSSuite struct {
	
}

var _ = Suite(&GCSSuite{})
var _ = fmt.Sprintf("hello!")

func (s *GCSSuite) TestGCSSuite(c *C) {
	bucket := "sply-test"
	prefix := "a"
	connection := NewGCSConnection(bucket, prefix)
	status := &NullStatusCallback{}
	files, err := connection.ListDir("", status)
	c.Assert(err, IsNil)
	//PrepareForRead(path string, etag string, localPath string, offset uint64, length uint64, status StatusCallback) (prepared *Region, err error) {
	localPath := c.MkDir()+"/dest"
	ioutil.WriteFile(localPath, make([]byte, 1000), 0700)	
	
	region, err := connection.PrepareForRead("DEADJOE", "*", localPath, 0, 10, status)
	c.Assert(region.Offset, Equals, 0)
	c.Assert(region.Length, Equals, 1000)
	fmt.Printf("files=%s\n", files)
}



