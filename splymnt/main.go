package main

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/gcfg.v1"

	_ "bazil.org/fuse/fs/fstestutil"
	"github.com/codegangsta/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "splymnt"
	app.Usage = "splymnt fuse client"
	app.Flags = []cli.Flag{
		&cli.StringFlag{Name: "dev", Value: "", Usage: "Enable development mode.  Root service, local service and fuse client all run in-process."},
	}

	// todo: add commands "mount", "status"

	app.Action = func(c *cli.Context) {
		mountpoint := c.Args().Get(0)
		fmt.Printf("mount point: %s\n", mountpoint)

		cfg := struct {
			S3 struct {
				AccessKeyId     string
				SecretAccessKey string
				Endpoint        string
				Bucket          string
				Prefix          string
			}
			Settings struct {
				Port        int
				PersistPath string
				AuthSecret  string
			}
		}{}

		fd, err := os.Open(devConfig)
		if err != nil {
			log.Fatalf("Could not open %s", devConfig)
		}
		err = gcfg.ReadInto(&cfg, fd)
		if err != nil {
			log.Fatalf("Failed to parse %s: %s", devConfig, err)
		}
		fd.Close()

	}

	app.Run(os.Args)
}
