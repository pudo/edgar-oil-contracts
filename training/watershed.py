import re
import random
from pprint import pprint
from unicodedata import normalize as ucnorm, category
from collections import defaultdict
import os

from nltk.util import ngrams
import nltk

REMOVE_SPACES = re.compile(r'\s+')
POS_DIR = 'data/positive'
NEG_DIR = 'data/negative'


def normalize_text(text):
    if not isinstance(text, unicode):
        text = unicode(text)
    chars = []
    # http://www.fileformat.info/info/unicode/category/index.htm
    for char in ucnorm('NFKD', text):
        cat = category(char)[0]
        if cat in ['C', 'Z', 'S']:
            chars.append(u' ')
        elif cat in ['M', 'P']:
            continue
        else:
            chars.append(char)
    text = u''.join(chars)
    text = REMOVE_SPACES.sub(' ', text)
    return text.strip().lower()


def read_texts(directory):
    for fn in os.listdir(directory):
        fn = os.path.join(directory, fn)
        with open(fn, 'r') as fh:
            text = fh.read()
            try:
                text = text.decode('utf-8')
                norm = normalize_text(text)
                for n in [1, 2, 3, 4, 5, 6]:
                    for grams in ngrams(norm.split(), n):
                        gt = ' '.join(grams)
                        yield fn, gt
            except UnicodeDecodeError:
                print "FAIL", fn


def run():
    features = defaultdict(set)
    for fn, gt in read_texts(POS_DIR):
        features[gt].add(fn)

    for fn, gt in read_texts(NEG_DIR):
        if gt in features:
            del features[gt]

    fs = [(gt, len(fns)) for gt, fns in features.items()]
    fs = sorted(fs, key=lambda (gt, fn): fn, reverse=True)
    for gt, fn in fs:
        if fn > 3:
            print "%s,%s" % (gt, fn)


if __name__ == '__main__':
    run()
