import requests
from lxml import html
import os


def get_id(link):
    p, d = link.split('/data/')
    d = d.split('/', 2)
    d = d[1].replace('.txt', '')
    return d.replace('-', '')


skips = []
with open('skip.txt', 'r') as fh:
    for line in fh.readlines():
        line = line.strip()
        skips.append(get_id(line))

print skips


with open('negatives.txt', 'r') as fh:
    for line in fh.readlines():
        line = line.strip()
        _id = get_id(line)
        if _id in skips:
            continue
        if line.endswith('.xml') or line.endswith('.xsd'):
            continue
        print _id, line
        path = 'data/negative/%s.txt' % _id
        if os.path.exists(path):
            continue
        res = requests.get(line)
        text = res.content.decode('utf-8')
        if line.endswith('.htm'):
            doc = html.fromstring(text)
            text = doc.xpath('string()')
        with open(path, 'w') as fh:
            fh.write(text.encode('utf-8'))

    #print lines
