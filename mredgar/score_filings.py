import re, math, string
from unicodedata import normalize as ucnorm, category

from mrjob.job import MRJob, JSONProtocol

DOCS = re.compile(r'^<DOCUMENT>$(.*)^</DOCUMENT>$', re.I | re.M | re.S)
SIC_EXTRACT = re.compile(r'<ASSIGNED-SIC> *(.*)', re.I)
REMOVE_SPACES = re.compile(r'\s+')

STOPWORDS = set(open('stopwords.txt').read().lower().split())
SCORES = {}

for line in open('searches.txt').readlines():
    term, score = line.rsplit(' ', 1)
    term = term.lower().strip()
    SCORES[term] = re.compile(term), float(score)

SEARCHES = re.compile(' (%s) ' % '|'.join(SCORES.keys()))


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


def get_tokens(text):
    tokens = []
    for token in text.split():
        if token in STOPWORDS:
            continue
        if string.digits in token:
            continue
        tokens.append(token)
    return tokens


def compute_score(doc):
    text = normalize_text(doc)
    terms = {}
    score = 0.0

    tokens = len(get_tokens(text))
    textlen = float(len(text))
    if tokens > 5:
        for match in SEARCHES.finditer(text):
            term = match.group(1)
            weight = 1
            if term in SCORES:
                _, weight = SCORES[term]
            else:
                for term_, (rex, weight_) in SCORES.items():
                    if rex.match(term):
                        weight = weight_
                        term = term_
                        break

            pos = match.start(1) / textlen
            weight = weight * ((-1 * pos) + 1)
            score = score + weight

            if term in terms:
                terms[term] += weight
            else:
                terms[term] = weight

    score = (score / tokens) * 1000
    return score, terms


class MRScoreFilings(MRJob):

    INPUT_PROTOCOL = JSONProtocol
    OUTPUT_PROTOCOL = JSONProtocol

    def mapper(self, x, line):
        for docmatch in DOCS.finditer(line):
            doc = docmatch.group(1)
            score, terms = compute_score(doc)
            if score > 0:
                print score, terms






if __name__ == '__main__':
    MRScoreFilings.run()
