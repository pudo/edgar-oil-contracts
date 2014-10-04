import os
import ftplib

import dataset
from docstash import Stash
from scrapekit import Scraper
from lxml import etree

HOST = 'ftp.sec.gov'
BASE_DIR = 'edgar/monthly'
EDGNS = '{http://www.sec.gov/Archives/edgar}'
SICS = [1381, 1382, 1389, 2911, 2990, 3532,
        3533, 5171, 5172, 6792, None]

scraper = Scraper('sec-edgar')
collection = Stash().get('edgar-filings')
db_uri = os.environ.get('DATABASE_URI', 'postgresql://localhost/secedgar')
engine = dataset.connect(db_uri)


@scraper.task
def monthly_indexes():
    ftp = ftplib.FTP(HOST)
    ftp.login('anonymous', '@anonymous')
    ftp.cwd(BASE_DIR)
    for file_name in ftp.nlst():
        path = os.path.join(scraper.config.data_path, file_name)
        if not os.path.exists(path):
            with open(path, 'wb') as fh:
                ftp.retrbinary("RETR " + file_name, fh.write)
        parse_feed.queue(path)
    ftp.quit()


@scraper.task
def parse_feed(file_name):
    doc = etree.parse(file_name)
    for item in doc.findall('.//item'):
        data = {}
        for c in item.iterchildren():
            if EDGNS in c.tag:
                continue
            if c.tag == 'enclosure':
                data[c.tag] = c.get('url')
            else:
                data[c.tag] = c.text

        for fc in item.findall(EDGNS + 'xbrlFiling/*'):
            tag = fc.tag.replace(EDGNS, '')
            if tag == 'xbrlFiles':
                continue
            if fc.text:
                data[tag] = fc.text

        engine['filings'].upsert(data, ['guid'])
        if data.get('assignedSic') is not None and int(data['assignedSic']) not in SICS:
            continue

        for fc in item.findall(EDGNS + 'xbrlFiling/' + EDGNS + 'xbrlFiles/*'):
            file_data = data.copy()
            file_rec = {'guid': data.get('guid')}
            for k, v in fc.attrib.items():
                file_data[k.replace(EDGNS, 'xbrlFile_')] = v
                file_rec[k.replace(EDGNS, '')] = v
            scraper.log.info('XBRL Filing: %s', file_data.get('xbrlFile_url'))
            engine['files'].upsert(file_rec, ['guid', 'url'])
            collection.ingest(file_data.get('xbrlFile_url'), **file_data)


if __name__ == '__main__':
    monthly_indexes.run()
