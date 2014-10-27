import re
from pprint import pprint
from unicodedata import normalize as ucnorm, category
from collections import defaultdict
import os

from nltk.util import ngrams
import nltk

REMOVE_SPACES = re.compile(r'\s+')
DIRS = [('data/positive', 'positive'), ('data/negative', 'negative')]
POS_DIR = 'data/positive'
NEG_DIR = 'data/negative'

#GRAMS = defaultdict(set)


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


def process_text(text):
    features = defaultdict(int)
    norm = normalize_text(text)
    for n in [3, 4, 5, 6]:
        for grams in ngrams(norm.split(), n):
            gt = ' '.join(grams)
            features[gt] += 1
    return features


def read_texts():
    for directory, category in DIRS:
        for fn in os.listdir(directory):
            fn = os.path.join(directory, fn)
            with open(fn, 'r') as fh:
                text = fh.read().decode('utf-8')
                yield fn, category, text


def train():
    classifier = nltk.NaiveBayesClassifier()
    print classifier



if __name__ == '__main__':
    train()
    #read_dir(POS_DIR, 'positive')
    #read_dir(NEG_DIR, 'negative')
    #top = sorted(GRAMS.items(), key=lambda (g, n): len(n), reverse=True)[:10]
    #pprint(top)
