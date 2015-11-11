package main

import (
	"os"

	_ "bazil.org/fuse/fs/fstestutil"
	"github.com/codegangsta/cli"
	"github.com/pgm/singleply"
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

				fs := singleply.NewFileSystem(&singleply.MockConn{},
					cache,
					singleply.NewTracker())

				singleply.StartMount(mountPoint, fs)
			}}}

	app.Run(os.Args)
}
