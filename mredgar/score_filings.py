import re, math, string
from unicodedata import normalize as ucnorm, category

from mrjob.job import MRJob, JSONProtocol

DOCS = re.compile(r'^<DOCUMENT>$(.*)^</DOCUMENT>$', re.I | re.M | re.S)
SIC_EXTRACT = re.compile(r'<ASSIGNED-SIC> *(.*)', re.I)
AN_EXTRACT = re.compile(r'<ACCESSION-NUMBER> *(.*)', re.I)
CIK_EXTRACT = re.compile(r'<CIK> *(.*)', re.I)
CN_EXTRACT = re.compile(r'<CONFORMED-NAME> *(.*)', re.I)
TYPE_EXTRACT = re.compile(r'<TYPE> *(.*)', re.I)
REMOVE_SPACES = re.compile(r'\s+')

URL = 'http://www.sec.gov/Archives/edgar/data/%s/%s/%s-index.htm'

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
            front_heavy = weight * ((-1 * pos) + 1)
            score = score + front_heavy + weight

            if term in terms:
                terms[term] += weight
            else:
                terms[term] = weight

    score = (score / tokens) * 1000
    return score, terms

# http://www.sec.gov/Archives/edgar/data/1402281/000135448810000906/0001354488-10-000906-index.htm

class MRScoreFilings(MRJob):

    INPUT_PROTOCOL = JSONProtocol
    OUTPUT_PROTOCOL = JSONProtocol

    def mapper(self, fn, data):
        score, terms = compute_score(data.get('doc'))
        if score <= 0:
            return
        an = AN_EXTRACT.findall(data.get('header'))
        if len(an) != 1:
            return
        an = an.pop()
        man = an.replace('-', '')
        sic = SIC_EXTRACT.findall(data.get('header')).pop()
        cik = CIK_EXTRACT.findall(data.get('header')).pop()
        url = URL % (int(cik), man, an)
        yield url, {
            'number': an,
            'cik': cik,
            'sic': sic,
            'filing_type': TYPE_EXTRACT.findall(data.get('header')).pop(),
            'doc_type': TYPE_EXTRACT.findall(data.get('doc')).pop(),
            'name': CN_EXTRACT.findall(data.get('header')).pop(),
            'score': score,
            'terms': terms
        }

            


if __name__ == '__main__':
    MRScoreFilings.run()
