package singleply

import (
        "fmt"
        "log"

        "golang.org/x/net/context"
        "golang.org/x/oauth2/google"
        storage "google.golang.org/api/storage/v1"
)

func listAllObjects(service *storage.ObjectsService, bucketName string, prefix string, callback func(objects *storage.Objects) error) error {
        pageToken := ""
        for {
                call := service.List(bucketName).Delimiter("/").Prefix(prefix)
                if pageToken != "" {
                        call = call.PageToken(pageToken)
                }
                res, err := call.Do()
                if err != nil {
                        return err
                }
                callback(res)
                if pageToken = res.NextPageToken; pageToken == "" {
                        break
                }
        }
        return nil        
}

type GCSConnection struct {
	bucket string
	prefix string
	service *storage.ObjectsService
}


func (c *GCSConnection) ListDir(context context.Context, path string, status StatusCallback) (*DirEntries, error) {
	files := make([]*FileStat, 0, 100)
	if path != "" {
		path = path + "/"
	}
	prefix := c.prefix + "/" + path
	fmt.Printf("ListDir(prefix=\"%s\")\n", prefix)
        
	// Handle cases where there are objects with keys like "dir/".  "dir" will be both a key and a common prefix
	// Not sure if this is a bug in fakes3 though, because there should be no key with the name "dir".  However,
	// filtering to avoid issues with both key and directry with same name
	dirNames := make(map[string]string)
        
	err := listAllObjects(c.service, c.bucket, prefix, func(objects *storage.Objects) error {
		for _, p := range objects.Prefixes {
			name := p
			name = name[len(prefix) : len(name)-1]

			fmt.Printf("Adding dir \"%s\" for prefix %s\n", name, p)
			files = append(files, &FileStat{Name: name, IsDir: true, Size: uint64(0)})
			dirNames[name] = name
		}

		for _, object := range objects.Items {
			name := object.Name[len(prefix):]

			if _, present := dirNames[name]; present {
				fmt.Printf("Skipping file %s for key %s because a dir with that name exists\n", name, object.Name)
				continue
			}

			fmt.Printf("Adding file \"%s\" for key \"%s\"\n", name, object.Name)
			isDir := false

			if name == "" {
				//name = "INVALID"
				continue
			}

			files = append(files, &FileStat{Name: name, IsDir: isDir, Size: uint64(object.Size), Etag: object.Etag})
		}

		return nil
                
        })

	if err != nil {
		return nil, err
	}

	return &DirEntries{Files: files}, nil
}

func NewGCSConnection(bucket string, prefix string) *GCSConnection {
	// config, err := google.ConfigFromJSON(jsonKey)
	
	// oauthHttpClient := &http.Client{
	// 	Transport: &oauth2.Transport{
	// 		Source: c.TokenSource(ctx),
	// 	}

	// storageService, err := storage.New(oauthHttpClient)

	//, scope
	client, err := google.DefaultClient(context.Background())
	if err != nil {
			log.Fatalf("Unable to get default client: %v", err)
	}
	service, err := storage.New(client)
	if err != nil {
			log.Fatalf("Unable to create storage service: %v", err)
	}

	// config := aws.NewConfig().WithCredentials(creds).WithEndpoint(endpoint).WithRegion(region).WithS3ForcePathStyle(true)

	// svc := s3.New(config)

	return &GCSConnection{bucket: bucket, prefix: prefix, service: service.Objects}
}

func (c *GCSConnection) PrepareForRead(context context.Context, path string, etag string, localPath string, offset uint64, length uint64, status StatusCallback) (prepared *Region, err error) {
	key := c.prefix + "/" + path

	// TODO: Add
	//	IfMatch: &etag,
	//	Key:   &key,
	//	Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))}
	
	res, err := c.service.Get(c.bucket, key).IfMatch(etag).Range(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)).Download()

	if err != nil {
		if(isStatusCode(err, 412)) {
			return nil, UpdateDetected
		}
		return nil, err
	}
		
	err = copyTo(localPath, offset, uint64(res.ContentLength), res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}

	return &Region{offset, uint64(res.ContentLength)}, err
}
