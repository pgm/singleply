package singleply

import (
	"log"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	//_ "bazil.org/fuse/fs/fstestutil"
	// "github.com/pgm/pliant/v2"
	// "github.com/pgm/pliant/v2/startup"
	// "github.com/pgm/pliant/v2/tagsvc"
)

func StartMount(mountpoint string, filesystem *FS) {
	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("pliantfuse"),
		fuse.Subtype("pliant"),
		fuse.LocalVolume(),
		fuse.VolumeName("pliant"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = fs.Serve(c, filesystem)
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}
