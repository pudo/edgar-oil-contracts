
PAT = 'aws --profile openoil s3 cp s3://sec-edgar.openoil.net/filings-13xx-2/%s s3://sec-edgar.openoil.net/filings-sample/%s'

with open('mrparts.txt', 'r') as fh:
    for i, line in enumerate(fh.readlines()):
        if i % 20 == 0:
            ts, fname = line.strip().rsplit(' ', 1)
            print PAT % (fname, fname)
