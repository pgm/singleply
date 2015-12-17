A caching FUSE layer for providing read-only access to cloud storage.

# Goal

Transparent, read-only access to objects in cloud storage providers such as GCS and S3.

There are many similar existing tools, however singleply makes a single (large) compromise in order to improve performance.   Specifically, singleply assumes that all objects are static and immutable.

This allows for not only caching of object contents, but directory listings are cached as well.

## Caching

All reads are first downloaded and stored locally, and then cached locally.  Similarly, all directory listings are serialized to disk.  

Reads are divided into blocks and cached as such.   (That is to say, if you only read 1k of a 1GB file, it will only download a single 1MB region (which contained the 1k request) and store that locally.)

When a file is first accessed, the ETag of the object is recorded.  The ETag is verified on every read from the object.  If the ETag has changed, that implies the contents of the object has changed
and the read is considered failed.

