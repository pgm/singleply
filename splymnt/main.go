package main

import (
	"log"
	"os"

	_ "bazil.org/fuse/fs/fstestutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/codegangsta/cli"
	"github.com/pgm/singleply"

	gcfg "gopkg.in/gcfg.v1"
)

func main() {
	app := cli.NewApp()
	app.Name = "splymnt"
	app.Usage = "splymnt fuse client"
	// app.Flags = []cli.Flag{
	// 	&cli.StringFlag{Name: "dev", Value: "", Usage: "Enable development mode.  Root service, local service and fuse client all run in-process."},
	// }

	// todo: add commands "mount", "status"
	app.Commands = []cli.Command{
		{
			Name:  "mount",
			Usage: "mount",
			Action: func(c *cli.Context) {
				cacheDir := c.Args().Get(0)
				mountPoint := c.Args().Get(1)
				cache, err := singleply.NewLocalCache(cacheDir)
				if err != nil {
					panic(err.Error())
				}

				connection := &singleply.MockConn{}
				fs := singleply.NewFileSystem(connection,
					cache,
					singleply.NewTracker())

				singleply.StartMount(mountPoint, fs)
			}},
		{
			Name:  "s3mount",
			Usage: "s3mount",
			Action: func(c *cli.Context) {
				configFile := c.Args().Get(0)

				cfg := struct {
					S3 struct {
						AccessKeyId     string
						SecretAccessKey string
						Endpoint        string
						Bucket          string
						Prefix          string
						Region          string
					}
					Settings struct {
						MountPoint string
						CacheDir   string
					}
				}{}

				fd, err := os.Open(configFile)
				if err != nil {
					log.Fatalf("Could not open %s", configFile)
				}
				err = gcfg.ReadInto(&cfg, fd)
				if err != nil {
					log.Fatalf("Failed to parse %s: %s", configFile, err)
				}
				fd.Close()

				cache, err := singleply.NewLocalCache(cfg.Settings.CacheDir)
				if err != nil {
					panic(err.Error())
				}

				s3creds := credentials.NewStaticCredentials(cfg.S3.AccessKeyId, cfg.S3.SecretAccessKey, "")
				connection := singleply.NewS3Connection(s3creds, cfg.S3.Bucket, cfg.S3.Prefix, cfg.S3.Region, cfg.S3.Endpoint)
				fs := singleply.NewFileSystem(connection,
					cache,
					singleply.NewTracker())

				singleply.StartMount(cfg.Settings.MountPoint, fs)
			}}}

	app.Run(os.Args)
}
