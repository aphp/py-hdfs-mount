import errno
import logging
import os
import sys
import tempfile

from docopt import docopt
from fuse import FUSE, FuseOSError, Operations
from hdfs import HdfsError
from hdfs.client import Client
from hdfs.ext.kerberos import KerberosClient
import yaml

from utils import stat_to_attrs


log = logging.getLogger()

while log.handlers:
    log.handlers.pop()

ch = logging.StreamHandler(sys.stdout)
# ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

HDFS_BLOCK_SIZE = 2 * 27


class HDFS(Operations):
    def __init__(self, hdfs_client: KerberosClient, hdfs_root, hdfs_user, hdfs_group):
        self.hdfs_client = hdfs_client
        self.hdfs_root = hdfs_root

        self.hdfs_user = hdfs_user
        self.hdfs_group = hdfs_group

        self.uid = os.getuid()
        self.gid = os.getgid()

        self._cache = {
            'last_cmd': '',
            'readdir': {
                'last_path': '',
                'last_resp': {},
                'count_get': 0
            }
        }

        # Bidirectional hashtable
        self.file_handle_fh = {}
        self.file_handle_p = {}

    # Helpers
    # =======

    def _full_path(self, partial):
        partial = partial.lstrip("/")
        path = os.path.join(self.hdfs_root, partial)
        return path

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        log.debug('access({}, {})'.format(path, mode))
        full_path = self._full_path(path)

        stat = self.hdfs_client.status(full_path)

        # TODO:
        # if not has_access(stat, mode):
        #     log.debug("path {} not accessible with rights {}".format(path, oct(mode)))
        #     raise FuseOSError(errno.EACCES)

        self._cache['last_cmd'] = 'access'

    def chmod(self, path, mode):
        log.debug('chmod({}, {})'.format(path, mode))

        full_path = self._full_path(path)

        try:
            self.hdfs_client.set_permission(full_path, permission=oct(mode)[-3:])
        except HdfsError as e:
            if e.exception == 'FileNotFoundException':
                raise FuseOSError(errno.ENOENT)

        self._cache['last_cmd'] = 'chmod'

    def chown(self, path, uid, gid):
        log.debug('chown({}, {})'.format(uid, gid))
        full_path = self._full_path(path)
        self._cache['last_cmd'] = 'chown'
        raise FuseOSError(errno.ENOSYS)
        # return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        """
        Return file attributes. The "stat" structure is described in detail in the stat(2) manual page. For the given pathname, this should fill in the elements of the "stat" structure. If a field is meaningless or semi-meaningless (e.g., st_ino) then it should be set to 0 or given a "reasonable" value. This call is pretty much required for a usable filesystem.
        """
        log.debug('getattr({}, {})'.format(path, fh))

        full_path = self._full_path(path)

        # Use the cache if readdir has been call just before on the parent directory of the current path
        parent_path = '/' + '/'.join(path.lstrip('/').split('/')[:-1])
        if path != parent_path and self._cache['last_cmd'] == 'readdir' and self._cache['readdir'][
            'last_path'] == parent_path:
            if self._cache['readdir']['count_get'] >= len(self._cache['readdir']['last_resp']):
                # TODO?
                pass
            try:
                elem = self._cache['readdir']['last_resp'][path.split('/')[-1]]
            except KeyError:
                raise FuseOSError(errno.ENOENT)
            self._cache['readdir']['count_get'] += 1
            return stat_to_attrs(elem, self.hdfs_user, self.hdfs_group)

        try:
            stat = self.hdfs_client.status(full_path)
        except HdfsError:
            raise FuseOSError(errno.ENOENT)

        self._cache['last_cmd'] = 'getattr'
        return stat_to_attrs(stat, self.hdfs_user, self.hdfs_group)

    def readdir(self, path, fh):
        """
        Return one or more directory entries (struct dirent) to the caller. This is one of the most complex FUSE functions.
        It is related to, but not identical to, the readdir(2) and getdents(2) system calls, and the readdir(3) library function.
        Because of its complexity, it is described separately below.
        Required for essentially any filesystem, since it's what makes ls and a whole bunch of other things work.
        """
        log.debug('readdir({}, {})'.format(path, fh))

        full_path = self._full_path(path)

        try:
            resp = self.hdfs_client.list(full_path, status=True)
        except HdfsError:
            raise FuseOSError(errno.EACCES)
        # ls = [a for a, _ in resp]
        ls_stat = {b['pathSuffix']: b for _, b in resp}

        self._cache['readdir']['last_path'] = path
        self._cache['readdir']['last_resp'] = ls_stat
        self._cache['readdir']['count_get'] = 0

        # FIXME: needed? (this does not seem to be a problem to omit that when browsing the FS)
        # yield '.', to_attrs(stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR, 0, 0, 0, 0, 0, 0, 0), 0
        # yield '..', to_attrs(stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR, 0, 0, 0, 0, 0, 0, 0), 0

        self._cache['last_cmd'] = 'readdir'
        for i, r in enumerate(resp):
            attrs = stat_to_attrs(r[1], self.hdfs_user, self.hdfs_group)
            # FIXME: what to return for the third parameter? Always zero?
            yield r[0], attrs, 0  # len(path.lstrip('/').split('/'))-1

    def readlink(self, path):
        log.debug('readlink({})'.format(path))
        self._cache['last_cmd'] = 'readlink'
        raise FuseOSError(errno.ENOSYS)
        # TODO:
        # pathname = os.readlink(self._full_path(path))
        # if pathname.startswith("/"):
        #     # Path name is absolute, sanitize it.
        #     return os.path.relpath(pathname, self.hdfs_root)
        # else:
        #     return pathname

    def mknod(self, path, mode, dev):
        log.debug('mknod({}, {}, {})'.format(path, mode, dev))
        #
        # full_path = self._full_path(path)

        self._cache['last_cmd'] = 'mknod'
        # TODO:
        # raise FuseOSError(errno.ENOSYS)
        # return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        log.debug('rmdir({})'.format(path))

        full_path = self._full_path(path)

        self._cache['last_cmd'] = 'rmdir'

        try:
            self.hdfs_client.delete(full_path, recursive=True)
        except HdfsError:
            raise FuseOSError(errno.ENOENT)

    def mkdir(self, path, mode):
        log.debug('mkdir({}, {})'.format(path, mode))

        full_path = self._full_path(path)

        self.hdfs_client.makedirs(full_path, permission=oct(mode)[-3:])

        self._cache['last_cmd'] = 'mkdir'
        return 0

    def statfs(self, path):
        log.debug('statfs({})'.format(path))
        full_path = self._full_path(path)
        self._cache['last_cmd'] = 'statfs'
        raise FuseOSError(errno.ENOSYS)
        # stv = os.statvfs(full_path)
        # return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
        #                                                  'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files',
        #                                                  'f_flag',
        #                                                  'f_frsize', 'f_namemax'))

    def unlink(self, path):
        log.debug('unlink({})'.format(path))
        full_path = self._full_path(path)

        self._cache['last_cmd'] = 'unlink'
        try:
            self.hdfs_client.delete(full_path, recursive=False)
        except HdfsError:
            raise FuseOSError(errno.ENOENT)

    def symlink(self, name, target):
        log.debug('symlink({}, {})'.format(name, target))
        self._cache['last_cmd'] = 'symlink'
        raise FuseOSError(errno.ENOSYS)
        # return os.symlink(name, self._full_path(target))

    def rename(self, old, new):
        log.debug('rename({}, {})'.format(old, new))

        full_old_path = self._full_path(old)
        full_new_path = self._full_path(new)

        try:
            self.hdfs_client.rename(full_old_path, full_new_path)
        except HdfsError as e:
            if e.exception == 'AccessControlException':
                raise FuseOSError(errno.EACCES)
            log.debug("Unhandled exception: ", e.exception)
            raise FuseOSError(errno.ENOSYS)

        self._cache['last_cmd'] = 'rename'

    def link(self, target, name):
        log.debug('link({}, {})'.format(target, name))
        self._cache['last_cmd'] = 'link'
        raise FuseOSError(errno.ENOSYS)
        # return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        log.debug('utimens({}, {})'.format(path, times))

        full_path = self._full_path(path)

        at = int(times[0] * 1000)
        mt = int(times[1] * 1000)

        try:
            self.hdfs_client.set_times(full_path, access_time=at, modification_time=mt)
        except HdfsError as e:
            if e.exception == 'IOException':
                log.debug(e)
                raise FuseOSError(errno.ENOSYS)

        self._cache['last_cmd'] = 'utimens'
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def _open(self, full_path, size, is_new_file):

        fh = 42
        while fh in self.file_handle_fh:
            fh += 1

        self.file_handle_fh[fh] = {
            'full_path': full_path,
            'actions': [],
        }

        if full_path in self.file_handle_p:
            self.file_handle_p[full_path]['fhs'].append(fh)
        else:
            tf = tempfile.TemporaryFile()
            tf.truncate(size)
            self.file_handle_p[full_path] = {
                'fhs': [fh],
                'tmp': tf,
                'written_parts': [],
                'is_new_file': is_new_file
            }

        return fh

    def _check_is_open(self, full_path, fh=None):
        if full_path not in self.file_handle_p or (fh is not None and fh not in self.file_handle_p[full_path]['fhs']):
            raise FuseOSError(errno.ENOENT)

    def open(self, path, flags):
        """
        Opens an already existing file.
        :param path:
        :param flags:
        :return: The file descriptor (int)
        """
        log.debug('open({}, {})'.format(path, flags))
        full_path = self._full_path(path)

        fh = self._open(full_path, self.getattr(path)['st_size'], is_new_file=False)

        self._cache['last_cmd'] = 'open'
        return fh

    def create(self, path, mode, fi=None):
        """
        Creates and open a non-existing file.
        :param path:
        :param mode:
        :param fi:
        :return: The file descriptor (int)
        """
        log.debug('create({}, {}, {})'.format(path, mode, fi))
        full_path = self._full_path(path)

        fh = self._open(full_path, 0, is_new_file=True)

        # self.file_handle_fh[fh]['actions'].append(('create', (mode)))

        try:
            self.hdfs_client.write(
                full_path,
                data=b'',
                overwrite=False,
                permission=oct(mode)[-3:],
                blocksize=None,
                buffersize=None,
                append=None,
                encoding='utf-8'
            )
        except HdfsError as e:
            if e.exception == 'FileAlreadyExistsException':
                raise FuseOSError(errno.EEXIST)

        self._cache['last_cmd'] = 'create'
        return fh

    def _read_from_hdfs(self, hdfs_path, offset, length, buffer_size=None, chunk_size=2 ** 12):
        try:
            res = b''
            with self.hdfs_client.read(hdfs_path=hdfs_path,
                                       offset=offset,
                                       length=length,
                                       buffer_size=buffer_size,
                                       encoding=None,
                                       chunk_size=chunk_size,
                                       delimiter=None,
                                       progress=None) as f:
                for chunk in f:
                    res += chunk
            return res
        except HdfsError as e:
            if e.exception == 'EOFException':
                raise FuseOSError(errno.EFAULT)
            elif e.exception == 'IOException':
                raise FuseOSError(errno.EFAULT)
            log.debug("Unhandled exception: ", e.exception)
            raise FuseOSError(errno.ENOSYS)

    def _get_parts(self, parts, fs, fe):
        if len(parts) == 0:
            return [], []

        # First, merge parts:
        def merge(times):
            saved = list(times[0])
            for st, en in sorted([sorted(t) for t in times]):
                if st <= saved[1]:
                    saved[1] = max(saved[1], en)
                else:
                    yield tuple(saved)
                    saved[0] = st
                    saved[1] = en
            yield tuple(saved)

        merged_parts = list(merge(parts))

        read_from_tmp = []
        for ps, pe in merged_parts:
            if ps < fs:
                if pe < fs:
                    rs, re = -1, -1
                else:
                    if pe > fe:
                        rs, re = fs, fe
                    else:
                        rs, re = fs, pe
            else:
                if ps > fe:
                    rs, re = -1, -1
                else:
                    if pe < fe:
                        rs, re = ps, pe
                    else:
                        rs, re = ps, fe

            if rs == -1:
                continue

            read_from_tmp.append((ps, pe, rs, re))
        read_from_hdfs = []
        if len(read_from_tmp) > 0:
            cp = fs
            for _, _, a, b in sorted(read_from_tmp, key=lambda x: x[3]):
                assert cp <= a
                if a != cp:
                    read_from_hdfs.append((cp, a - 1))
                cp = b + 1
        return read_from_tmp, read_from_hdfs

    def read(self, path, length, offset, fh):
        log.debug('read({}, {}, {}, {})'.format(path, length, offset, fh))
        full_path = self._full_path(path)
        self._check_is_open(full_path, fh)

        result = b'\0' * length

        # Find out where:
        # -> we read from hdfs
        # -> we read from the temporary file that already has written parts

        read_from_tmp, read_from_hdfs = self._get_parts(
            self.file_handle_p[full_path]['written_parts'],
            offset,
            offset + length)

        for ps, pe, rs, re in read_from_tmp:
            self.file_handle_p[full_path]['tmp'].seek(rs)
            result[ps:pe] = self.file_handle_p[full_path]['tmp'].read(rs - re)

        if len(read_from_tmp) > 0:
            for a, b in read_from_tmp:
                result[a:b] = self._read_from_hdfs(full_path, a, b - a)
        else:
            result = self._read_from_hdfs(full_path, offset, length)

        self._cache['last_cmd'] = 'read'
        return result

    def write(self, path, buf, offset, fh):
        log.debug('write({}, {}, {}, {})'.format(path, buf, offset, fh))
        full_path = self._full_path(path)
        self._check_is_open(full_path, fh)

        self._cache['last_cmd'] = 'write'

        self.file_handle_fh[fh]['actions'].append(('write', (offset, buf)))

        return len(buf)

    def truncate(self, path, length, fh=None):
        """
        This method truncates a file to the given length.
        It is also applying the result immediately (flush).
        :param path:
        :param length:
        :param fh:
        :return:
        """

        log.debug('truncate({}, {}, {})'.format(path, length, fh))
        full_path = self._full_path(path)

        # if length>current_length, add \0 bytes

        if fh is None:
            fh = self._open(full_path, length, is_new_file=False)
            self.truncate(path, length, fh)
            self.flush(path, fh)
            self.release(path, fh)
        else:
            self._check_is_open(full_path, fh)

            self.file_handle_p[full_path]['tmp'].truncate(length)
            self.file_handle_p[full_path]['tmp'].flush()
            os.fsync(self.file_handle_p[full_path]['tmp'].fileno())

        self._cache['last_cmd'] = 'truncate'

        return 0

    def flush(self, path, fh):
        log.debug('flush({}, {})'.format(path, fh))
        full_path = self._full_path(path)
        self._check_is_open(full_path, fh)

        for action in self.file_handle_fh[fh]['actions']:
            if action[0] == 'write':
                offset, buf = action[1]

                self.file_handle_p[full_path]['tmp'].seek(offset)
                self.file_handle_p[full_path]['tmp'].write(buf)
                self.file_handle_p[full_path]['tmp'].flush()
                os.fsync(self.file_handle_p[full_path]['tmp'].fileno())

                self.file_handle_p[full_path]['written_parts'].append((offset, offset + len(buf)))

        if len(self.file_handle_fh[fh]['actions']) > 0:
            self.fsync(path, None, fh)
        self.file_handle_fh[fh]['actions'] = []

        self._cache['last_cmd'] = 'flush'
        return 0

    def fsync(self, path, fdatasync, fh):
        log.debug('fsync({}, {}, {})'.format(path, fdatasync, fh))
        self._cache['last_cmd'] = 'fsync'
        full_path = self._full_path(path)
        self._check_is_open(full_path, fh)

        self.file_handle_p[full_path]['tmp'].seek(0, 2)
        size = self.file_handle_p[full_path]['tmp'].tell()

        read_from_tmp, read_from_hdfs = self._get_parts(self.file_handle_p[full_path]['written_parts'], 0, size)

        if len(read_from_tmp) > 0:
            for a, b in read_from_hdfs:
                self.file_handle_p[full_path]['tmp'].seek(a)
                self.file_handle_p[full_path]['tmp'].write(self._read_from_hdfs(full_path, a, b))

            self.file_handle_p[full_path]['tmp'].seek(0)
            data = self.file_handle_p[full_path]['tmp'].read()

            try:
                return self.hdfs_client.write(
                    full_path,
                    data=data,
                    overwrite=True,
                    permission="755",
                    blocksize=None,
                    buffersize=None,
                    append=None,
                    encoding=None,
                )
            except HdfsError as e:
                log.debug("Unhandled exception: ", e.exception)
                raise FuseOSError(errno.ENOSYS)

    def release(self, path, fh):
        log.debug('release({}, {})'.format(path, fh))
        self._cache['last_cmd'] = 'release'
        full_path = self._full_path(path)
        self._check_is_open(full_path, fh)

        self.file_handle_p[full_path]['fhs'].remove(fh)

        del self.file_handle_fh[fh]

        if len(self.file_handle_p[full_path]['fhs']) == 0:
            del self.file_handle_p[full_path]

        return 0
 
if __name__ == '__main__':
    doc = """A simple program to mount HDFS as a linux filesystem (using FUSEpy).
    
    Usage: {0} [--loglevel LEVEL]  CONFIG
    
    Options:
      --loglevel=<level> The log level [default: INFO]
      --version  Show program version
    
    """.format(sys.argv[0])
 
    args = docopt(doc, version="0.1", help=False)
    log_level = args['--loglevel']
    if log_level == 'DEBUG':
        log.setLevel(logging.DEBUG)
    elif log_level == 'INFO' or log_level is None:
        log.setLevel(logging.INFO)

    with open(args['CONFIG'], 'r') as f:
        cfg = yaml.load(f)

    hdfs_server = cfg['hdfs']['server']
    hdfs_mount_root = cfg['hdfs']['mount_root']
    mount_dest_dir = cfg['mount']['dest_dir']

    if not os.path.isdir(mount_dest_dir):
        print('Directory {0} does not exists, please specify an existing directory.'.format(mount_dest_dir))
        exit(1)

    if cfg['hdfs']['kerberos']:
        hdfs_client = KerberosClient(hdfs_server)
    else:
        hdfs_client = Client(hdfs_server)

    operations = HDFS(hdfs_client, hdfs_mount_root, None, None)
    FUSE(operations, mountpoint=mount_dest_dir, raw_fi=False, nothreads=True, foreground=True)
