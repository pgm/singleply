package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	// "time"
	
	"golang.org/x/net/context"
	"bazil.org/fuse/fs"	
	"bazil.org/fuse"

	_ "bazil.org/fuse/fs/fstestutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/codegangsta/cli"
	"github.com/pgm/singleply"

	gcfg "gopkg.in/gcfg.v1"
)

type SplyClient struct {
	stats   *singleply.Stats
	tracker *singleply.Tracker
	cache   singleply.Cache
	server  *fs.Server
	fs      *singleply.FS
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

/*
func getNode(ctx context.Context, f fs.FS, path string) fs.Node {
	node, err := f.Root()
	//TODO: split and iterate
	name := path
	node, err = node.(fs.NodeStringLookuper).Lookup(ctx, name)
	return node
}

func (c *SplyClient) GetLocal(args *string, result **string) error {
	node := getNode(ctx, c.fs, args)
	var req  *fuse.ReadRequest
	var resp fuse.ReadResponse
	err := node.(HandleReader).Read(ctx, req, &resp)
	asString := string(b)
	*result = &asString
	return nil
}
*/

func (c *SplyClient) Invalidate(path string, result **string) error {
	fmt.Printf("Calling c.cache.Invalidate\n")
	err := c.cache.Invalidate(path)
	if err == nil {
		fmt.Printf("Calling fs.Invalidate\n")
		err = c.fs.Invalidate(context.Background(), c.server, path)
	}

	var r string
	if err != nil && err != fuse.ErrNotCached {
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
	GCS struct {
		Prefix string
		Bucket string
	}
	Settings struct {
		MountPoint  string
		CacheDir    string
		ControlFile string
		DelaySecs   int
		Workers     int
		FetchSize   int
	}
}

func loadConfig(c *cli.Context) *Config {
	return _loadConfig( c.GlobalString("config") )
	
}

func _loadConfig(configFile string) *Config {
	cfg := Config{}
	cfg.Settings.Workers = 5
	cfg.Settings.FetchSize = 1024*1024

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
	
	// TODO: use ~/.splyctl as config file by default and add command line arg to override 
	// Streamline logging strategy
	// support invalidate
	
	app.Flags = []cli.Flag{
	 	&cli.StringFlag{Name: "config", Value: "~/.splyctl", Usage: "Path to config file"},
	}

	// todo: add commands "mount", "status"
	app.Commands = []cli.Command{
		{
			Name:  "invalidate",
			Usage: "invalidate relpath",
			Action: func(c *cli.Context) {
				path := c.Args().Get(0)
				cfg := loadConfig(c)
				client := ConnectToServer(cfg.Settings.ControlFile)
				var result *string
				err := client.Call("SplyClient.Invalidate", path, &result)
				if err != nil {
					log.Fatalf("SplyClient.Invalidate failed: %s", err.Error())
				}
				fmt.Printf("status: %s\n", *result)
			}},
		{
			Name:  "local",
			Usage: "invalidate configfile relpath",
			Action: func(c *cli.Context) {
				path := c.Args().Get(0)
				cfg := loadConfig(c)
				client := ConnectToServer(cfg.Settings.ControlFile)
				var result *string
				err := client.Call("SplyClient.GetLocal", path, &result)
				if err != nil {
					log.Fatalf("SplyClient.GetLocal failed: %s", err.Error())
				}
				fmt.Printf("status: %s\n", *result)
			}},
		{
			Name:  "status",
			Usage: "status configfile",
			Action: func(c *cli.Context) {
				cfg := loadConfig(c)
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
			Usage: "stats configfile",
			Action: func(c *cli.Context) {
				cfg := loadConfig(c)
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
				cfg := loadConfig(c)

				cache, err := singleply.NewLocalCache(cfg.Settings.CacheDir)
				if err != nil {
					panic(err.Error())
				}

				var connection singleply.Connector
				if cfg.GCS.Bucket != "" {
					connection = singleply.NewGCSConnection(cfg.GCS.Bucket, cfg.GCS.Prefix)
				} else if cfg.S3.Bucket != "" {
					s3creds := credentials.NewStaticCredentials(cfg.S3.AccessKeyId, cfg.S3.SecretAccessKey, "")
					connection = singleply.NewS3Connection(s3creds, cfg.S3.Bucket, cfg.S3.Prefix, cfg.S3.Region, cfg.S3.Endpoint)
				} else {
					panic("Needed either GCS bucket or S3 bucket selected")
				}

				// if cfg.Settings.DelaySecs != 0 {
				// 	connection = singleply.DelayConnector(time.Second*time.Duration(cfg.Settings.DelaySecs), connection)
				// }

				stats := &singleply.Stats{}
				tracker := singleply.NewTracker()
				fs := singleply.NewFileSystem(connection,
					cache,
					tracker,
					stats,
					10,
					1*1024*1024)

				log.Printf("StartMount\n")
				fc, server, serveCompleted := singleply.StartMount(cfg.Settings.MountPoint, fs)
				defer fc.Close()
				log.Printf("StartMount completed\n")

				client := SplyClient{stats: stats, tracker: tracker, cache: cache, server: server, fs: fs}

				_, err = StartServer(cfg.Settings.ControlFile, &client)
				if err != nil {
					panic(err.Error())
				}

				log.Printf("StartServer completed\n")

				// check if the mount process has an error to report
				<-fc.Ready
				if err := fc.MountError; err != nil {
					log.Fatal(err)
				}
				// wait until server is shut down
				<-serveCompleted

			}}}

	app.Run(os.Args)
}
