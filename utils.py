from pwd import getpwnam
import stat

import os


# "stat" structure :
#
#   0 dev      the device number of the filesystem
#   1 ino      the inode number
#   2 mode     the file mode  (type and permissions)
#   3 nlink    the number of (hard) links to the file
#   4 uid      the numeric user ID of file's owner
#   5 gid      the numeric group ID of file's owner
#   6 rdev     the device identifier (special files only)
#   7 size     the total size of the file, in bytes
#   8 atime    the last access time in seconds since the epoch
#   9 mtime    the last modify time in seconds since the epoch
#  10 ctime    the inode change time in seconds since the epoch (*)
#  11 blksize  the preferred block size for file system I/O
#  12 blocks   the actual number of blocks allocated

def to_st_mode(permission, type):
    node_type = None
    if type == 'DIRECTORY':
        node_type = stat.S_IFDIR
    elif type == 'FILE':
        node_type = stat.S_IFREG

    if node_type is None:
        raise NotImplementedError('type {} not implemented!'.format(type))
    # log.debug(str(permission) + ' ' + str(type) + ' ' + str(int(permission, 8) | node_type))
    return int(permission, 8) | node_type


def get_user_info(username):
    try:
        ui = getpwnam(username)
        return ui.pw_uid, ui.pw_gid
    except KeyError:
        # log.warning('No rights mapping')
        return 55555, 55555


def to_attrs(st_mode, st_uid, st_gid, st_size, st_atime, st_mtime, st_ctime, st_nlink=0):
    return {
        'st_mode': st_mode,  # 2 the file mode (type and permissions)
        'st_nlink': st_nlink,  # 3 the number of (hard) links to the file
        'st_uid': st_uid,  # 4 the numeric user ID of file's owner
        'st_gid': st_gid,  # 5 the numeric group ID of file's owner
        'st_size': st_size,  # 7 the total size of the file, in bytes
        'st_atime': st_atime,  # 8 the last access time in seconds since the epoch
        'st_mtime': st_mtime,  # 9 the last modify time in seconds since the epoch
        'st_ctime': st_ctime,  # 10 the inode change time in seconds since the epoch (*)
    }


def stat_to_attrs(stat, hdfs_user, hdfs_group):
    if stat['owner'] == hdfs_user:
        uid = os.getuid()
        if stat['group'] == hdfs_user:
            gid = os.getgid()
        elif stat['group'] == hdfs_group:
            gid = os.getgid()
        else:
            gid = 0
    else:
        uid, gid = 0, 0
    # uid, gid = get_user_info(stat['owner'])
    return to_attrs(
        st_mode=to_st_mode(stat['permission'], stat['type']),
        st_uid=uid,
        st_gid=gid,
        st_size=stat['length'],
        st_atime=stat['accessTime'],
        st_mtime=stat['modificationTime'],
        st_ctime=stat['modificationTime'],
    )


def has_access(stat, mode):
    st_mode = to_st_mode(stat['permission'], stat['type'])
    return st_mode & mode > 0
