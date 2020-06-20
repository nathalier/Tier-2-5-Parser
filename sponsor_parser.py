from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import requests
from db_insert import insert_to_db


def find_sponsors_url(source_url):
    r = requests.get(source_url)
    soup = BeautifulSoup(r.content, features="lxml")
    sponsors_link = soup.find('section', {'id': 'documents'}). \
        find('div', {'class': 'attachment-details'}).find('a').get('href')
    return sponsors_link


def get_sponsors_parsed(pdf_url):
    doc_date = re.search('\/20(\d{2}-\d{2}-\d{2})', pdf_url).groups()[0].replace('-', '')
    base_dir = 'sponsors'
    file_name_prefix = 'tier-2-5_sponsors'
    with open(f'./{base_dir}/{file_name_prefix}_{doc_date}.pdf', 'wb') as f:
        f.write(requests.get(pdf_url).content)
    with open(f'./{base_dir}/{file_name_prefix}_{doc_date}.pdf', 'rb') as in_f,\
         open(f'./{base_dir}/{file_name_prefix}_{doc_date}.xml', 'w', encoding='utf-8') as out_f:
        # options: -q : quiet, -i : ignore images
        cmd = f'pdftohtml -xml -enc UTF-8 -q -i {in_f.name} {out_f.name}'
        os.system(cmd)
    return f'./{base_dir}/{file_name_prefix}_{doc_date}.xml', doc_date


class SponsorsData:
    """Converts provided XML file with sponsor list to DF and saves it as csv file"""

    tier_subtypes = {'Creative & Sporting', 'Tier 2 General', 'Seasonal Worker',
                     'Intra Company Transfers (ICT)', 'Religious Workers', 'Voluntary Workers',
                     'Exchange', 'Sport', 'International Agreements'}
    counties = set(pd.read_csv('uk-counties-list.csv', header=None)[1])

    def __init__(self, file_path:str, date:str=None, encoding='utf-8', write_df=True):
        self.file_encoding = encoding
        self.date = date or self._parse_date(file_path)
        if file_path.endswith('.xml'):
            self.xml_data_file = file_path
            self.csv_data_file = f'{self.xml_data_file[:-4]}.csv'
            self.sponsors_df = self._xml_to_df()
        elif file_path.endswith('.csv'):
            self.csv_data_file = file_path
            self.sponsors_df = pd.read_csv(self.csv_data_file, encoding=encoding)
        else:
            raise ValueError(f'Incorrect file format: {file_path}. '
                             f'xml or csv file is expected')
        self.correct_df()
        if write_df:
            self._write_df_to_csv()

    @staticmethod
    def _parse_date(fname):
        return re.search('(\d{6})', fname).groups()[0]

    def _write_df_to_csv(self):
        self.sponsors_df.to_csv(self.csv_data_file, encoding='utf-8', index=False)

    def correct_df(self):
        if self.sponsors_df.loc[pd.isnull(self.sponsors_df['tier_type'])].size > 0:
            self.fix_missed_tier_type()

    def fix_missed_tier_type(self):
        def correct_tier_type(x):
            potential_type = x['tier_subtype'][:6]
            if potential_type == 'Tier 2' or potential_type == 'Tier 5':
                x['tier_type'] = potential_type
                return x
            else:
                raise ValueError(f"Tier visa type is not defined: {x}")

        self.sponsors_df.loc[pd.isnull(self.sponsors_df['tier_type'])] = \
            self.sponsors_df.loc[pd.isnull(self.sponsors_df['tier_type'])].\
            apply(correct_tier_type, axis=1)

    def diff(self, other):
        if not isinstance(other, SponsorsData):
            raise TypeError(f'SponsorsData object is expected for comparison. {type(other)} got.')
        merged = self.sponsors_df.merge(other.sponsors_df, indicator=True, how='outer')
        path = f"{self.csv_data_file[0:self.csv_data_file.rfind('/')]}/" \
               f"diff_{self.date}_{other.date}.csv"
        merged[merged['_merge'] != 'both'].sort_values('name').\
            to_csv(path, index=False)
        # print(merged[merged['_merge'] == 'right_only'])
        # print(merged[merged['_merge'] == 'left_only'])

    def _xml_to_df(self):
        with open(self.xml_data_file, encoding=self.file_encoding) as f:
            file = f.read()
        soup = BeautifulSoup(file, features='lxml')
        start_tag = soup.find('text', string='County')
        column_names = {'County', 'Tier & Rating', 'Organisation Name', 'Sub Tier', 'Town/City'}

        def next_info_tag(tag):
            tag = tag.find_next('text')
            if tag is None: return tag
            while tag.text in column_names or re.match('^Page \d* of \d*$', tag.text):
                tag = tag.find_next('text')
            if tag and tag.text == 'Total' and tag.find_next('text').text == 'Tier 2':
                tag = None
            return tag

        sponsors = []
        tag = next_info_tag(start_tag)
        curr_row = {}
        tier_fields = {'tier_rating', 'tier_type', 'tier_subtype'}
        while tag:
            if tag.find('b'):
                curr_row['name'] = tag.text
            else:
                print(f'SOMETHING WRONG: {tag} is not  organization name')
                tag = next_info_tag(start_tag)
                continue
            tag = next_info_tag(tag)
            next_tag = next_info_tag(tag)
            if (next_tag.text in self.tier_subtypes) or (next_tag.text.startswith('Tier ')):
                curr_row['city'] = tag.text
                tag = next_tag
            else:
                country, city = tag.text.strip('., '), next_tag.text.strip('., ')
                if city in self.counties:
                    city, country = country, city
                curr_row['county'] = country
                curr_row['city'] = city
                tag = next_info_tag(next_tag)

            tier_options = [{}]  # list of visa types dict for the current organization
            while tag and not tag.find('b'):
                type_rate = re.search('(Tier \d).*?\((.*)\)', tag.text)
                if type_rate:
                    tier_options[-1]['tier_type'] = type_rate.groups()[0]
                    tier_options[-1]['tier_rating'] = type_rate.groups()[1]
                    tag = next_info_tag(tag)
                    if ('tier_subtype' not in tier_options[-1]) and (not tag.find('b')):
                        continue
                elif tag.text in self.tier_subtypes:
                    tier_options[-1]['tier_subtype'] = tag.text
                    tag = next_info_tag(tag)
                    if ('tier_type' not in tier_options[-1]) and (not tag.find('b')):
                        continue
                else:
                    print(f'Unknown Tier type: {tag.text}')
                    tag = next_info_tag(tag)
                tier_options.append({})
            for tier_option in tier_options[:-1]:
                for field in tier_fields:
                    if field not in tier_option:
                        print(f'{tier_fields.difference(set(tier_option.keys()))} '
                              f'unknown for the organization: {curr_row}')
                        break
                sponsors.append({**curr_row, **tier_option})

            curr_row = {}

        columns = ['name', 'city', 'county', 'tier_type', 'tier_rating', 'tier_subtype']
        sponsors = pd.DataFrame(sponsors, columns=columns)
        return sponsors


def main():
    gov_url = r'https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers'
    sponsors_url = find_sponsors_url(gov_url)
    file_path, file_date = get_sponsors_parsed(sponsors_url)
    if os.path.isfile(file_path.replace('.xml', '.csv')):
        print(f'Data for date {file_date} already loaded.')
    else:
        sd = SponsorsData(file_path, date=file_date)
        insert_to_db(sd.csv_data_file)


if __name__ == "__main__":
    main()
