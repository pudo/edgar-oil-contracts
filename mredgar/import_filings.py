import logging
import re
import os
import shutil
import tempfile
import tarfile
import ftplib

from mrjob.job import MRJob, RawValueProtocol, JSONProtocol

log = logging.getLogger('importer')
HOST = 'ftp.sec.gov'

SIC_EXTRACT = re.compile(r'<ASSIGNED-SIC> *(.*)', re.I)
DOC_IN = re.compile(r'^<DOCUMENT>\s*$', re.I)
DOC_OUT = re.compile(r'^</DOCUMENT>\s*$', re.I)
SICS = ['1311', '1381', '1382', '1389']


def match_sic(line):
    sic = None
    for match in SIC_EXTRACT.finditer(line):
        sic = match.group(1)
        if sic in SICS:
            return True
    return False


def split_filing(dir, line, file_name):
    path = os.path.join(dir, file_name)
    
    with open(path, 'r') as fh:
        is_header = True
        header = []
        doc_idx = 0
        doc = None
        for tline in fh.readlines():
            if doc is not None:
                doc.append(tline)
            
            if DOC_IN.match(tline):
                if is_header:
                    is_header = False
                    header = ''.join(header)
                    if not match_sic(header):
                        return
                doc = [tline]
            elif DOC_OUT.match(tline):
                fn = '%s:%s:%s' % (line, file_name, doc_idx)
                yield fn, {
                    'header': header,
                    'doc': ''.join(doc),
                    'file_name': file_name,
                    'doc_idx': doc_idx
                }
                doc = None
                doc_idx += 1

            if is_header:
                header.append(tline)


class MRImportFilings(MRJob):

    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = JSONProtocol

    def mapper(self, x, line):
        fd, tarpath = tempfile.mkstemp()
        os.close(fd)
        dir = tempfile.mkdtemp()

        try:
            ftp = ftplib.FTP(HOST)
            ftp.login('anonymous', '@anonymous')
            with open(tarpath, 'wb') as fh:
                ftp.retrbinary("RETR " + line, fh.write)
            ftp.quit()

            tb = tarfile.open(tarpath, 'r:gz')
            tb.extractall(path=dir)
            for file_name in os.listdir(dir):
                for res in split_filing(dir, line, file_name):
                    #print res
                    yield res
                
        except EnvironmentError, ee:
            log.exception(ee)
        except Exception, e:
            log.exception(e)
        finally:
            shutil.rmtree(dir)
            os.unlink(tarpath)

    #def reducer(self, key, values):
    #    yield key, values


if __name__ == '__main__':
    MRImportFilings.run()
