import json
import os
import dataset

DIR = 'scores'
eng = dataset.connect('sqlite://')
entries = eng['filings']

for fn in os.listdir(DIR):
    path = os.path.join(DIR, fn)
    if 'part' not in fn:
        continue
    with open(path, 'r') as fh:
        for line in fh.readlines():
            key, value = line.split('\t', 1)
            row = json.loads(value)
            row['filing_url'] = json.loads(key)
            terms = row.pop('terms')
            match_info = []
            for term, weight in terms.items():
                match_info.append('%s: %s' % (term, weight))
            row['match_info'] = ', '.join(match_info)
            entries.insert(row)

dataset.freeze(entries, format='csv', filename='scored.csv')

