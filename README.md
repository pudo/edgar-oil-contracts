# SEC EDGAR Oil Contracts Finder

This project aims to find full oil contract bodies among the filings submitted by
oil companies to the American stock regulator.

We've used a variety of approaches:

* Download and store SEC filings in the appropriate SIC classes using a Hadoop
  cluster. The resulting corpus is a JSON stream with an entry for each
  document filed since 1995.
* Score the documents using a second Hadoop cluster by counting terms that
  indicate an oil contract. The score is considered both normalized over
  the number of total words in the filed document, and as an absolute
  number (we actually want to bias for long texts).

We've also used a set of confirmed postive and negative matches to
generate a set of "watershed" terms which occur only in the contract
documents and not in any others. This was used to generate a search list
automatically, for a second phase of ranking.

Contact:

* @Open_Oil, @pudo
