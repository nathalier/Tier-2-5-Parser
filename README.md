# Tier-2-5 sponsors parser
The program downloads pdf file with Tier-2 and Tier-5 sponsor list from the official source (gov.uk).
Then program converts downloaded pdf to xml, parses it to dataframe, saves as csv and inserts parsed result into Postgresql tier-2-5-sponsors DB. 

## Usage
run sponsor_parser.py [xml_or_csv_sponsors_file]

## Specific Dependencies

### PDFTOHTML
Pdftohtml should be installed and added to path on the system in order to run the script.
http://pdftohtml.sourceforge.net


