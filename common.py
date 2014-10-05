import os

import dataset
from docstash import Stash

collection = Stash().get('edgar-filings')
db_uri = os.environ.get('DATABASE_URI', 'postgresql://localhost/secedgar')
engine = dataset.connect(db_uri)
