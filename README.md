# Tier-2-5 sponsors parser
The program downloads pdf file with Tier-2 and Tier-5 sponsor list from the official source (gov.uk).
Then program converts downloaded pdf to xml, parses it to dataframe, saves as csv and inserts parsed result into Postgresql tier-2-5-sponsors DB. 

## Usage
0. Install Pdftohtml (see dependencies) 
1. Rename connection_settings_template.py to connection_settings.py
2. [optional] Type db connection parameters into connection_settings.py.
If no db connection parameters provided inserting intodb step will be skipped.
3. run ```sponsor_parser.py``` to get freshest UK Tier-2 and Tier-5 sponsor list saved in csv file 
(and optionally inserted into your db)
3a. as an option you can add xml_or_csv_sponsors_file argument 
(```sponsor_parser.py xml_or_csv_sponsors_file```) to get this file converted to csv and/or inserted into db.

## Specific Dependencies

### PDFTOHTML
Pdftohtml should be installed and added to path on the system in order to run the script.
http://pdftohtml.sourceforge.net


