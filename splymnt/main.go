package main

import (
	"log"
	"fmt"
	"os"
	"encoding/json"
	"net/rpc"
	"net"

	_ "bazil.org/fuse/fs/fstestutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/codegangsta/cli"
	"github.com/pgm/singleply"

	gcfg "gopkg.in/gcfg.v1"
)

type SplyClient struct {
	stats *singleply.Stats
	tracker *singleply.Tracker
	cache singleply.Cache
}

func (c *SplyClient) GetStats(args *string, result **string) error {
	b, err := json.Marshal(c.stats)
	if err != nil {
		return err
	}
	asString := string(b)
	*result = &asString
	return nil
}

func (c *SplyClient) GetStatus(args *string, result **string) error {
	states := c.tracker.GetState()
	wrapper := struct {
		States []*singleply.State
	}{}
	wrapper.States = states
	
	b, err := json.Marshal(&wrapper)
	if err != nil {
		return err
	}
	asString := string(b)
	*result = &asString
	return nil
}

func (c *SplyClient) Invalidate(path string, result **string) error {
	err := c.cache.Invalidate(path)
	var r string
	if err != nil {
		r = err.Error()
	} else {
		c.stats.IncInvalidatedDirCount()
		r = "okay"
	}
	*result = &r	
	return nil
}

func ConnectToServer(addr string) *rpc.Client {
	client, err := rpc.Dial("unix", addr)
	if err != nil {
		panic(err.Error())
	}
	return client
}

func notifyWhenFinished(fn func()) chan int {
	completed := make(chan int)
	go (func() {
		fn()
		completed <- 1
	})()

	return completed
}

func StartServer(addr string, client *SplyClient) (chan int, error) {
	server := rpc.NewServer()
	server.Register(client)
	
	if _, err := os.Stat(addr); !os.IsNotExist(err) {
		err = os.Remove(addr)
		if err != nil {
			log.Fatalf("Failed to remove %s: %s", addr, err.Error())
		}
	}
	
	l, err := net.ListenUnix("unix", &net.UnixAddr{addr, "unix"})
	if err != nil {
		return nil, err
	}

	log.Printf("Ready to accept requests via %s\n", addr)
	return notifyWhenFinished(func() {
		server.Accept(l)
		os.Remove(addr)
	}), nil
}

type Config struct {
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
			ControlFile string
		}
	}

func loadConfig(configFile string) *Config {
	cfg := Config{}

	fd, err := os.Open(configFile)
	if err != nil {
		log.Fatalf("Could not open %s", configFile)
	}
	err = gcfg.ReadInto(&cfg, fd)
	if err != nil {
		log.Fatalf("Failed to parse %s: %s", configFile, err)
	}
	fd.Close()
	
	return &cfg	
}

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
			Name:  "invalidate",
			Usage: "invalidate",
			Action: func(c *cli.Context) {
				configFile := c.Args().Get(0)
				path := c.Args().Get(1)
				cfg := loadConfig(configFile)
				client := ConnectToServer(cfg.Settings.ControlFile)
				var result *string
				err := client.Call("SplyClient.Invalidate", path, &result)
				if err != nil {
					log.Fatalf("SplyClient.Invalidate failed: %s", err.Error())
				}
				fmt.Printf("status: %s\n", *result)
			}},
		{
			Name:  "status",
			Usage: "status",
			Action: func(c *cli.Context) {
				configFile := c.Args().Get(0)
				cfg := loadConfig(configFile)
				client := ConnectToServer(cfg.Settings.ControlFile)
				unused := ""
				var result *string
				err := client.Call("SplyClient.GetStatus", &unused, &result)
				if err != nil {
					log.Fatalf("SplyClient.GetStatus failed: %s", err.Error())
				}
				fmt.Printf("status: %s\n", *result)
			}},
		{
			Name:  "stats",
			Usage: "stats",
			Action: func(c *cli.Context) {
				configFile := c.Args().Get(0)
				cfg := loadConfig(configFile)
				client := ConnectToServer(cfg.Settings.ControlFile)
				unused := ""
				var result *string
				err := client.Call("SplyClient.GetStats", &unused, &result)
				if err != nil {
					log.Fatalf("SplyClient.GetStats failed: %s", err.Error())
				}
				fmt.Printf("stats: %s\n", *result)
			}},
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

				stats := &singleply.Stats{}
				connection := &singleply.MockConn{}
				fs := singleply.NewFileSystem(connection,
					cache,
					singleply.NewTracker(), stats)

				singleply.StartMount(mountPoint, fs)
			}},
		{
			Name:  "s3mount",
			Usage: "s3mount",
			Action: func(c *cli.Context) {
				configFile := c.Args().Get(0)

				cfg := loadConfig(configFile)

				cache, err := singleply.NewLocalCache(cfg.Settings.CacheDir)
				if err != nil {
					panic(err.Error())
				}

				stats := &singleply.Stats{}
				s3creds := credentials.NewStaticCredentials(cfg.S3.AccessKeyId, cfg.S3.SecretAccessKey, "")
				connection := singleply.NewS3Connection(s3creds, cfg.S3.Bucket, cfg.S3.Prefix, cfg.S3.Region, cfg.S3.Endpoint)
				tracker := singleply.NewTracker()
				fs := singleply.NewFileSystem(connection,
					cache,
					tracker,
					stats)

				client := SplyClient{stats: stats, tracker: tracker, cache: cache}

				_, err = StartServer(cfg.Settings.ControlFile, &client)
				if err != nil {
					panic(err.Error())
				}

				singleply.StartMount(cfg.Settings.MountPoint, fs)
			}}}

	app.Run(os.Args)
}
