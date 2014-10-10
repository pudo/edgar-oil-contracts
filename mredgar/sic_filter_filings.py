import re

from mrjob.job import MRJob, JSONProtocol

SIC_EXTRACT = re.compile(r'<ASSIGNED-SIC> *(.*)', re.I)
SICS = ['1311', '1381', '1382', '1389']


class MRSicFilterFilings(MRJob):

    INPUT_PROTOCOL = JSONProtocol
    OUTPUT_PROTOCOL = JSONProtocol

    def mapper(self, x, line):
        sic = None
        for match in SIC_EXTRACT.finditer(line):
            sic = match.group(1)
            #if len(sic) and sic not in SICS:
            if sic not in SICS:
                return
        if sic is None:
            return

        yield x, line


if __name__ == '__main__':
    MRSicFilterFilings.run()
