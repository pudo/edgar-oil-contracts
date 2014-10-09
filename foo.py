import re
import os
import shutil
import tempfile
import tarfile
from StringIO import StringIO

from mrjob.job import MRJob, RawValueProtocol


class MRFilingScore(MRJob):

    INPUT_PROTOCOL = RawValueProtocol

    def mapper(self, x, line):
        dir = tempfile.mkdtemp()
        print len(line)
        print x
        tb = tarfile.open(StringIO(line), 'r:gz')
        tb.extractall(path=dir)
        for file_name in os.listdir(dir):
            path = os.path.join(dir, file_name)
            with open(path, 'r') as fh:
                yield file_name, fh.read()
        shutil.rmtree(dir)
        #yield "len", len(line)

    def reducer(self, key, values):
        yield key, len(values)


if __name__ == '__main__':
    MRFilingScore.run()
