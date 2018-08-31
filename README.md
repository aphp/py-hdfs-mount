### Requirements

Python 3

### Install

```
sudo apt-get install fuse libfuse2
```

```
pip3 install -r requirements.txt
```

If you will be using kerberos, install libkrb5-dev:

```
sudo apt-get install libkrb5-dev
```

### Configuration

```
cp example.config.yaml config.yaml
$EDITOR config.yaml
```

### Running

If you are using kerberos, run a kinit:

```
kinit -kt $USER $USER@REALM
```

In all cases you then will have to create a new empty directory that with be the mount point:

```
mkdir /mnt/dest_mount
```

And finaly you can run py-hdfs-fuse:

```
python3 hdfs_mount.py [--loglevel LEVEL] config.yaml
```

Have fun!

Note: if anything goes wrong and you have to kill py-hdfs-mount, you will probably have to run this command on the mounted folder to unlock it:

```
fusermount -u /mnt/dest_mount
```


### Tested with


* [x] Vim (open file, edit randomly, save and close)
* [x] cp/mv

### Functionnalities


* [x] Cached writes (HDFS is an immutable FS (so writes=delete+insert))
* [x] Random writes (slow - because of the immutability of HDFS - but working!)
* [x] Very fast ls (cached directory metadata)
* [ ] directory stored as a zip file in HDFS (to solve small files problem)
* [ ] directory stored as a avro file in HDFS (to solve small files problem)
* [ ] CRC32 checksum
* [ ] Load options from configuration file


### Implemented FUSE methods

#### Basic
* [ ] access
* [x] chmod
* [ ] chown
* [x] getattr
* [x] readdir
* [ ] readlink
* [ ] mknod
* [x] rmdir
* [x] mkdir
* [ ] statfs
* [ ] unlink
* [ ] symlink
* [x] rename
* [ ] link
* [x] utimens

#### File methods

* [x] open
* [x] create
* [x] read
* [x] write (caching is done in memory)
* [x] truncate
* [x] flush (writes the in memory written chunks to a temporary file in the local FS in the right order and calls fsync)
* [x] fsync (send the temporary file to HDFS)
* [x] release

