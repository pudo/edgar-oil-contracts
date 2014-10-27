import re
import random
from pprint import pprint
from unicodedata import normalize as ucnorm, category
from collections import defaultdict
import os

from nltk.util import ngrams
import nltk

REMOVE_SPACES = re.compile(r'\s+')
DIRS = [('data/positive', 'positive'), ('data/negative', 'negative')]

# POS_DIR = 'data/positive'
# NEG_DIR = 'data/negative'

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


def text_features(text):
    features = defaultdict(int)
    norm = normalize_text(text)
    features['len(text)'] = len(text)
    features['len(norm)'] = len(norm)
    for n in [1, 2, 3]:
        for grams in ngrams(norm.split(), n):
            gt = ' '.join(grams)
            features['phrase(%s)' % gt] += 1
    return features


def read_texts():
    for directory, category in DIRS:
        for fn in os.listdir(directory):
            fn = os.path.join(directory, fn)
            with open(fn, 'r') as fh:
                text = fh.read()
                try:
                    text = text.decode('utf-8')
                    yield fn, category, text
                except UnicodeDecodeError:
                    print "FAIL", fn
                #yield fn, category, text


def train():
    print "Reading texts..."
    texts = list(read_texts())
    print "Generating n-grams..."
    featureset = [(text_features(t), c) for f, c, t in texts]
    random.shuffle(featureset)
    TEST_SIZE = 20
    train_set = featureset[len(featureset) - TEST_SIZE:]
    test_set = featureset[:len(featureset) - TEST_SIZE]
    print "Training classifier..."
    classifier = nltk.NaiveBayesClassifier.train(train_set)
    #print classifier
    #print "Features"
    classifier.show_most_informative_features(20)
    print "Testing classifier..."
    print nltk.classify.accuracy(classifier, test_set)
    import pickle
    with open('classifier.pickle', 'wb') as fh:
        pickle.dump(classifier, fh)
    

if __name__ == '__main__':
    train()
    #read_dir(POS_DIR, 'positive')
    #read_dir(NEG_DIR, 'negative')
    #top = sorted(GRAMS.items(), key=lambda (g, n): len(n), reverse=True)[:10]
    #pprint(top)
