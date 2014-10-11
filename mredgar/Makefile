
make_listing:
	python generate_input.py
	rm -rf tasks
	mkdir tasks
	split -l 10 listing.txt tasks/listing

test_import:
	python import_filings.py --cleanup=NONE --no-output <test_listing.txt

test_sic_filter:
	python sic_filter_filings.py --cleanup=NONE --no-output <filings.txt

test_score:
	python score_filings.py --cleanup=NONE --no-output <filings.txt	

import:
	python import_filings.py -r emr \
		--conf-path mrjob.conf \
		--output-dir=s3://sec-edgar.openoil.net/filings-13xx/ \
		--no-output \
		s3://sec-edgar.openoil.net/tasks/

sic_filter:
	python sic_filter_filings.py -r emr \
		--conf-path mrjob.conf \
		--output-dir=s3://sec-edgar.openoil.net/filings-13xx/ \
		--no-output \
		s3://sec-edgar.openoil.net/filings-all/

score:
	python score_filings.py -r emr \
		--conf-path mrjob.conf \
		--output-dir=s3://sec-edgar.openoil.net/scores/ \
		--no-output \
		--file searches.txt --file stopwords.txt \
		s3://sec-edgar.openoil.net/filings-13xx/
