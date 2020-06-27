from bs4 import BeautifulSoup
from glob import glob
import pandas as pd
import os
import re
import requests
from sqlalchemy import create_engine
import sys
from connection_settings import params


def find_sponsors_url(source_url):
    r = requests.get(source_url)
    soup = BeautifulSoup(r.content, features="lxml")
    sponsors_link = soup.find('section', {'id': 'documents'}). \
        find('div', {'class': 'attachment-details'}).find('a').get('href')
    return sponsors_link


def get_sponsors_parsed(pdf_url):
    doc_date = re.search('\/20(\d{2}-\d{2}-\d{2})', pdf_url).groups()[0].replace('-', '')
    # TODO create 'sponsors' dir if not exists
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


def freshest_data():
    # find all sponsors file and return the path to the freshest one
    files = glob('./sponsors/tier*.csv')
    files = sorted(files)
    return files[-1]


class SponsorsData:
    """Converts provided XML file with sponsor list to DF and saves it as csv file"""

    # form a list of tier type-subtype tuple for each subtype
    tier_type_subtypes = pd.read_csv('tier_types.csv').iloc[:, :2]
    tier_type_subtypes = [tuple(x) for x in tier_type_subtypes.values]
    # create a set of different tier subtypes
    tier_subtypes = set([x[1] for x in tier_type_subtypes if str(x[1]) != 'nan'])
    counties = set(pd.read_csv('uk-counties-list.csv', header=None)[1])

    def __init__(self, file_path:str, date:str=None, encoding='utf-8', to_csv=True,
                 to_db=True, check_errors=True, to_db_if_error=False, db_settings=None):
        if db_settings is None:
            to_db = False
        else:
            self.db_params = db_settings
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
        if to_csv:
            self._write_df_to_csv()
        # errors in data probable
        self.prob_error = False
        if check_errors:
            self.check_df()
        if to_db and ((not self.prob_error) or to_db_if_error):
            self.insert_into_db()

    @staticmethod
    def _parse_date(fname):
        return re.search('(\d{6})', fname).groups()[0]

    def _write_df_to_csv(self):
        self.sponsors_df.to_csv(self.csv_data_file, encoding='utf-8', index=False)

    def insert_into_db(self):
        conn_params = f"postgresql://{self.db_params['user']}:{self.db_params['password']}" \
                      f"@{self.db_params['host']}/{self.db_params['database']}"
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

    def correct_df(self):
        if self.sponsors_df.loc[pd.isnull(self.sponsors_df['tier_type'])].size > 0:
            self.fix_missed_tier_type()

    def check_df(self):
        # 1. check there are no unknown visa types
        if len(self.sponsors_df[~self.sponsors_df['tier_subtype'].isin(self.tier_subtypes)]) > 0:
            self.prob_error = True
            print(f'WARNING: '
                  f'{self.sponsors_df.tier_subtype[~self.sponsors_df.tier_subtype.isin(self.tier_subtypes)].values}'
                  f' are not in the tier subtypes list')

        # 2. check new sponsor list does not differ much (<20%?) from the previous one.
        def prev_data():
            # find all sponsors file and return the path to the last one
            files = glob('sponsors/tier*.csv')
            files = sorted(files)
            if self.csv_data_file in files:
                files.remove(self.csv_data_file)
            if files:
                return files[-1]
        last_data = prev_data()
        if not last_data:
            return
        last_sponsors = SponsorsData(last_data, encoding=self.file_encoding,
                                     to_csv=False, to_db=False, check_errors=False)
        if len(self.diff(last_sponsors).index) >= 0.4 * len(self.sponsors_df.index):
            print(f'New sponsor list differs too much (>20%) from the previous one')
            self.prob_error = True

    def fix_missed_tier_type(self):
        def correct_tier_type(x):
            if x['tier_subtype'] == '':
                print(f'No tier type/subtype defined')
                return x
            tier_type = [i[0] for i in self.tier_type_subtypes if i[1] == x['tier_subtype']]
            if tier_type:
                x['tier_type'] = tier_type[0]
                # print(f'tier visa subtype was corrected for :\n{x}')
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


def download():
    gov_url = r'https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers'
    sponsors_url = find_sponsors_url(gov_url)
    file_path, file_date = get_sponsors_parsed(sponsors_url)
    # if os.path.isfile(file_path.replace('.xml', '.csv')):
    #     print(f'Data for date {file_date} already loaded.')
    return file_path, file_date


if __name__ == "__main__":
    if len(sys.argv) == 1:
        file_path, file_date = download()
    else:
        file_path = sys.argv[1]
        file_date = re.search('_(\d{6})\.', file_path).groups()[0]
    sd = SponsorsData(file_path, date=file_date, to_db=True, check_errors=True, to_db_if_error=False, db_settings=params)
