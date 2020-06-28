from bs4 import BeautifulSoup
from glob import glob
import pandas as pd
import os
from pathlib import Path
import re
import requests
from sqlalchemy import create_engine
import sys
from connection_settings import params


gov_url = r'https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers'
rel_data_path = './sponsors'
Path(rel_data_path).mkdir(parents=True, exist_ok=True)
file_name_prefix = 'tier-2-5_sponsors'


class SponsorsData:
    """Converts provided XML file with sponsor list to DF and saves it as csv file"""

    # form a list of tier type-subtype tuple for each subtype
    tier_type_subtypes = pd.read_csv('tier_types.csv').iloc[:, :2]
    tier_type_subtypes = [tuple(x) for x in tier_type_subtypes.values]
    # create a set of different tier subtypes
    tier_subtypes = set([x[1] for x in tier_type_subtypes if str(x[1]) != 'nan'])
    counties = set(pd.read_csv('uk-counties-list.csv', header=None)[1])

    def __init__(self, file_path:str, date:str=None, encoding='utf-8'):
        self.file_encoding = encoding
        self.date = date or self._parse_date(file_path)
        if file_path.endswith('.xml'):
            self.xml_data_file = file_path
            self.csv_data_file = f'{self.xml_data_file[:-4]}.csv'
            self.sponsors_df = self._xml_to_df()
            self.correct()
            self._write_df_to_csv()
        elif file_path.endswith('.csv'):
            self.csv_data_file = file_path
            self.sponsors_df = pd.read_csv(self.csv_data_file, encoding=encoding)
        else:
            raise ValueError(f'Incorrect file format: {file_path}. '
                             f'xml or csv file is expected')

    @staticmethod
    def _parse_date(fname):
        return re.search('(\d{6})', fname).groups()[0]

    def _write_df_to_csv(self):
        self.sponsors_df.to_csv(self.csv_data_file, encoding=self.file_encoding, index=False)
        print('Sponsors file is successfully saved to CSV')

    def insert_into_db(self, db_params=params):
        conn_params = f"postgresql://{db_params['user']}:{db_params['password']}" \
                      f"@{db_params['host']}/{db_params['database']}"
        engine = create_engine(conn_params)

        # insert tier types into db
        df_tier_types = pd.read_csv('tier_types.csv')
        df_tier_types.index.name = 'tier_type_id'
        df_tier_types.to_sql(name='tier_types', con=engine, if_exists='replace', method='multi')

        # insert sponsors into db
        df_sponsors_unique = self.sponsors_df[['name', 'city', 'county']].drop_duplicates().reset_index(drop=True)
        df_sponsors_unique.index.name = 'sponsor_id'
        df_sponsors_unique.to_sql(name='sponsors', con=engine, if_exists='replace', method='multi')

        # fill sponsor-visa_type table (many to many relation)
        df_sponsors_unique['sponsor_id'] = df_sponsors_unique.index
        df_sponsors_with_id = pd.merge(self.sponsors_df, df_sponsors_unique, on=['name', 'city', 'county'])
        df_tier_types['tier_type_id'] = df_tier_types.index
        df_sponsors_visas = pd.merge(df_sponsors_with_id, df_tier_types, on=['tier_type', 'tier_subtype'])
        df_sponsors_visas[['sponsor_id', 'tier_type_id', 'tier_rating']]. \
            to_sql(name='sponsors_visas', con=engine, if_exists='replace', method='multi')

        print(f'Successfully inserted into DB')

        with engine.connect() as con:
            con.execute('GRANT SELECT ON ALL TABLES IN SCHEMA public TO guest;')

    def correct(self):
        # 1. In case of missed tier type calculate it from the subtype whenever possible
        if self.sponsors_df.loc[pd.isnull(self.sponsors_df['tier_type'])].size > 0:
            self.fix_missed_tier_type()

    def validate(self):
        no_errors = True
        # 1. check there are no unknown visa types
        if len(self.sponsors_df[~self.sponsors_df['tier_subtype'].isin(self.tier_subtypes)]) > 0:
            no_errors = False
            print(f'WARNING: '
                  f'{self.sponsors_df.tier_subtype[~self.sponsors_df.tier_subtype.isin(self.tier_subtypes)].values}'
                  f' are not in the tier subtypes list')
        return no_errors

    def fix_missed_tier_type(self):
        def correct_tier_type(x):
            if x['tier_subtype'] == '':
                print(f'No tier type/subtype defined')
                return x
            tier_type = [i[0] for i in self.tier_type_subtypes if i[1] == x['tier_subtype']]
            if tier_type:
                x['tier_type'] = tier_type[0]
                print(f'Missed data - tier visa type was corrected for :\n{x}')
            else:
                print(f"Unknown tier visa subtype: {x}")
                self.prob_error = True
            return x

        self.sponsors_df.loc[pd.isnull(self.sponsors_df['tier_type'])] = \
            self.sponsors_df.loc[pd.isnull(self.sponsors_df['tier_type'])].\
            apply(correct_tier_type, axis=1)

    def diff(self, other, to_write=True):
        if not isinstance(other, SponsorsData):
            raise TypeError(f'SponsorsData object is expected for comparison. {type(other)} got.')
        merged = self.sponsors_df.merge(other.sponsors_df, indicator=True, how='outer')
        path = f"{self.csv_data_file[0:self.csv_data_file.rfind('/')]}/" \
               f"diff_{self.date}_{other.date}.csv"
        if to_write:
            merged[merged['_merge'] != 'both'].sort_values('name').\
                to_csv(path, index=False)
        return merged[merged['_merge'] != 'both']
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
                    print(f'Tier type expected. {tag.text} got. Missed data probable.')
                    tag = next_info_tag(tag)
                tier_options.append({})
            for tier_option in tier_options[:-1]:
                for field in tier_fields:
                    if field not in tier_option:
                        print(f'{tier_fields.difference(set(tier_option.keys()))} '
                              f'missed for the organization: {curr_row}')
                        break
                sponsors.append({**curr_row, **tier_option})

            curr_row = {}

        columns = ['name', 'city', 'county', 'tier_type', 'tier_rating', 'tier_subtype']
        sponsors = pd.DataFrame(sponsors, columns=columns)
        print('XML sponsors file is successfully converted to DF')
        return sponsors


def find_sponsors_url(source_url):
    r = requests.get(source_url)
    soup = BeautifulSoup(r.content, features="lxml")
    sponsors_link = soup.find('section', {'id': 'documents'}). \
        find('div', {'class': 'attachment-details'}).find('a').get('href')
    return sponsors_link


def get_sponsors_parsed(pdf_url):
    doc_date = re.search('\/20(\d{2}-\d{2}-\d{2})', pdf_url).groups()[0].replace('-', '')
    file_path_no_ext = f'{rel_data_path}/{file_name_prefix}_{doc_date}'

    # no need to do anything if this data has already been downloaded (called from __main__)
    if Path(f'{file_path_no_ext}.csv').exists():
        print(f'{Path(file_path_no_ext).name} has already been processed')
        sys.exit(0)

    with open(f'{file_path_no_ext}.pdf', 'wb') as f:
        f.write(requests.get(pdf_url).content)
    print('PDF sponsors file downloaded')
    with open(f'{file_path_no_ext}.pdf', 'rb') as in_f,\
         open(f'{file_path_no_ext}.xml', 'w', encoding='utf-8') as out_f:
        # options: -q : quiet, -i : ignore images
        cmd = f'pdftohtml -xml -enc UTF-8 -q -i {in_f.name} {out_f.name}'
        os.system(cmd)
        print('PDF sponsors file successfully converted to XML')
    return f'{file_path_no_ext}.xml'


def download():
    sponsors_url = find_sponsors_url(gov_url)
    return get_sponsors_parsed(sponsors_url)


def last_data_files(n=2, ext='csv'):
    # find all sponsors file and return the path to the freshest one
    files = glob(f'{rel_data_path}/tier*.{ext}')
    files = sorted(files)
    return files[-n:]


def small_diff(sd):
    # check new sponsor list does not differ much (<20%?) from the previous one
    # take two last csv files
    sponsor_files = last_data_files()
    # it's expected that data we validate has (just) been saved in csv with the latest date in the file name
    assert re.sub('^./', '', sponsor_files[-1]) == re.sub('^./', '', sd.csv_data_file)
    # if no previous data found
    if len(sponsor_files) < 2:
        print('No prev data found to compare for difference')
        return True
    prev_sd = SponsorsData(sponsor_files[0])
    if len(sd.diff(prev_sd).index) >= 0.2 * (len(sd.sponsors_df.index) + len(prev_sd.sponsors_df.index)) / 2:
        print('New sponsor list differs too much (>20%) from the previous one')
        return False
    print(f'New sponsor list does not differ much from the previous one')
    return True


if __name__ == "__main__":
    if len(sys.argv) == 1:
        file_path = download()
    else:
        file_path = sys.argv[1]
    sd = SponsorsData(file_path)
    # if no new/suspicious data and it hasn't been changed too much from the previous one
    if sd.validate() and small_diff(sd):
        sd.insert_into_db(params)
