### Requirements

Python 3

### Install

```
pip install -r requirements.txt
```

### Running

```
kinit -kt $USER $USER@REALM

mkdir dest_mount
python hdfs_mount.py http://hdfs_hostname:50070 <hdfs-root-path> dest_mount <hdfs-user> <hdfs-group>
```

Have fun