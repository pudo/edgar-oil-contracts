import os
import ftplib

HOST = 'ftp.sec.gov'
BASE_PATH = '/edgar/Feed/'


def traverse(ftp, prefix):
    listing = []
    ftp.dir(prefix, listing.append)
    for entry in listing:
        _, fn = entry.rsplit(' ', 1)
        sub = os.path.join(prefix, fn)
        if fn == 'tmp':
            continue
        if entry.startswith('drwx'):
            for tb in traverse(ftp, sub):
                yield tb
        if '.tar.gz' in fn:
            yield sub


def archive_list():
    ftp = ftplib.FTP(HOST)
    ftp.login('anonymous', '@anonymous')
    for entry in traverse(ftp, BASE_PATH):
        print entry
    ftp.quit()


archive_list()
