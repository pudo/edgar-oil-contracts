
make_listing:
	python generate_input.py
	rm -rf tasks
	mkdir tasks
	split -l 10 listing.txt tasks/listing

test_import:
	python import_filings.py --cleanup=NONE --no-output <test_listing.txt

test_process:
	python process_filings.py --cleanup=NONE --no-output <filings.txt


import:
	python import_filings.py -r emr \
		--conf-path mrjob.conf \
		--output-dir=s3://sec-edgar.openoil.net/filings/ \
		--no-output \
		s3://sec-edgar.openoil.net/listing.txt
