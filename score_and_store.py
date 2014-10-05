import re
import string

from common import engine, collection


table = engine['matches']

STOPWORDS = set(open('stopwords.txt').read().lower().split())
SEARCHES = '|'.join(open('searches.txt').read().lower().split())
SEARCHES = re.compile(' (%s) ' % SEARCHES)


def get_tokens(text):
    tokens = []
    for token in text.split():
        if token in STOPWORDS:
            continue
        if string.digits in token:
            continue
        tokens.append(token)
    return tokens


def process_document(doc):
    guid = doc.get('guid')
    if guid is None:
        return
    text = doc.get('normalized', '')
    tokens = len(get_tokens(text))
    if tokens < 5:
        return
    matches = SEARCHES.findall(text)
    if len(matches) < 1:
        return
    
    #print doc.keys()
    data = {
        'guid': guid,
        'link': doc.get('link'),
        'source_url': doc.get('source_url'),
        'num_tokens': tokens,
        'num_matches': len(matches),
        'matches': ', '.join(sorted(set(matches))),
        'score': (float(len(matches)) / tokens) * 1000
    }
    table.upsert(data, ['source_url'])
    print data['link'], data['score']
    #print data
    #print doc.get('link'), tokens, len(matches), (float(len(matches)) / tokens) * 1000


if __name__ == '__main__':
    for doc in collection:
        process_document(doc)
