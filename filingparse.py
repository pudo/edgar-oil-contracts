import re
import os
import shutil
import tempfile
import tarfile

test_file = 'data/19951006.nc.tar.gz'


def process_archive(archive_file):
    dir = tempfile.mkdtemp()
    tb = tarfile.open(archive_file, 'r:gz')
    tb.extractall(path=dir)
    for file_name in os.listdir(dir):
        path = os.path.join(dir, file_name)
        with open(path, 'r') as fh:
            process_filing(fh.read())
    shutil.rmtree(dir)


def process_filing(filing):
    print len(filing)


if __name__ == '__main__':
    process_archive('data/19951006.nc.tar.gz')
