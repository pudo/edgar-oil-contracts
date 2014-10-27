from nltk.util import ngrams
import nltk
import pickle

with open('classifier.pickle', 'rb') as fh:
    classifier = pickle.load(fh)

    print dir(classifier)
    
