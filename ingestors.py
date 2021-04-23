# Copyright 2021 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Cameron Neylon

"""
Ingestors for Diversity Data

Conceptually an ingestor takes a file and processes it to generate long-form data containing observations of
counts of particular demographic groups. There are no guarantees that groups are disjunct and combining of
groups must be done with knowledge of the source data. Normalisation to COKI/Observatory categories is done
elsewhere.

Categories should reflect the original datasets in as un-modified a manor as possible. Some metadata on category
types and information is also carried with the record of events.

An ingestor should return a flat, long form and fully populated dataframe with the following columns:

* year - the census date for the data, generally reflecting the end of the collection period. If this is not
directly available from the file itself it should be set to None for cleanup later
* source - the data source in standardised form eg us_ipeds, or au_det
* source_institution_id - ideally a unique ID found in the data source such as a UKPRN, IPEDS ID or HEMIS ID.
* source_institution_name - The name of the institution where this is not available, eg Australia
* source_category_name - the category name (or names as a list) from the original data
* source_category_value - the demographic category (or categories as a list)) from the original data
* counts - the counts provided in the original data
* source_year_type - One of 'calendar', 'nh_academic', 'unknown'
* source_count_type - One of 'fte', 'headcount', 'unknown
"""

import pandas as pd
import numpy as np

from walk import DataFile, Walker, REGEXES


def ingest_us_ipeds(file: DataFile):
    """Take a provided filepath and process to generate back a semi-standardised pandas dataframe"""

    melt_id_vars = ['unit_id', 'institution_name', 'occupation', 'occupation_filled']
    # Post 2015 files
    if file.year >= 2015:
        engine = 'openpyxl'
        source_data = pd.read_excel(file.filepath, header=4, na_values='-', engine=engine)
        category_types = ['occupation_filled', 'gender', 'ethnicity']
        melt_id_vars.extend(['gender'])
        melt_var_name = 'ethnicity'


    elif file.year < 2015:
        if file.filepath.suffix == '.csv':
            source_data = pd.read_csv(file.filepath, header=0)
        elif file.filepath.suffix == '.xlsx':
            engine = 'openpyxl'
            source_data = pd.read_excel(file.filepath, engine=engine)
        category_types = ['occupation_filled', 'gender and ethnicity']
        # Find the name of the column that has "Occupation" or "occupational activity" in its title
        occupation_column_name = source_data.columns[
            ['ccupation' in colname for colname in source_data.columns].index(True)]
        source_data['occupation'] = source_data[occupation_column_name]
        melt_id_vars.extend(['year'])
        melt_var_name = 'gender and ethnicity'

    cleaned_column_names = {k: k.lower().replace(' ', '_') for k in source_data.columns}
    cleaned_column_names.update(dict(unitid='unit_id'))
    source_data.rename(columns=cleaned_column_names, inplace=True)
    source_data['occupation_filled'] = source_data.occupation.ffill()

    melted = source_data.melt(id_vars=melt_id_vars,
                              var_name=melt_var_name,
                              value_name='counts')

    category_values = melted[category_types].apply(list, axis='columns')
    out_df = pd.DataFrame(dict(year=[None] * len(melted),
                               source_institution_id=melted.unit_id.astype(str),
                               source_institution_name=melted.institution_name,
                               source=[file.source] * len(melted),
                               source_category_type=[category_types] * len(melted),
                               source_category_value=category_values,
                               count=melted['counts'])
                          )
    if 'year' in melted.columns:
        out_df['year'] = melted.year
    else:
        out_df['year'] = [file.year] * len(melted)
    return out_df


def ingest_au_det(file: DataFile):
    source_data = pd.read_excel(file.filepath, header=2, skipfooter=5, engine='openpyxl')
    source_data[source_data.columns[0]].ffill(inplace=True)
    source_data[0:1] = source_data[0:1].ffill(axis='columns')

    num_lines = len(source_data[2:])
    long_df = pd.DataFrame()
    for column in source_data.columns[2:]:
        year = [source_data[column].values[1]] * num_lines
        gender = [source_data[column].values[0]] * num_lines
        current_duties_classification = source_data[source_data.columns[0]][2:]
        source_name = source_data[source_data.columns[1]][2:]
        count = source_data[column][2:]
        long_df = long_df.append(pd.DataFrame(dict(year=year,
                                                   gender=gender,
                                                   current_duties_classification=current_duties_classification,
                                                   source_name=source_name,
                                                   count=count)))

    long_df['lower_name'] = long_df.source_name.str.lower()
    long_df.lower_name = long_df.lower_name.str.replace('the ', '')
    long_df.lower_name = long_df.lower_name.str.replace(',', '')
    category_types = ['current_duties_classification', 'gender']
    category_values = long_df[category_types].apply(list, axis='columns')
    out_df = pd.DataFrame(dict(year=long_df.year,
                               source_institution_id=long_df.lower_name,
                               source_institution_name=long_df.source_name,
                               source=[file.source] * len(long_df),
                               source_category_type=[category_types] * len(long_df),
                               source_category_value=category_values,
                               count=long_df['counts']))

    return out_df


def ingest_au_indigenous(file: DataFile):
    if file.filepath.suffix == '.xlsx':
        engine = 'openpyxl'
    else:
        engine = None

    if file.year > 2007:
        sheetnames = ['1', '2']
        header = [2, 3]
    elif file.year == 2003:
        sheetnames = ['21', '22']
        header = [3, 4]
    elif file.year > 2003:
        sheetnames = ['21', '22']
        header = [2, 3]
    else:
        sheetnames = ['22', '23']
        header = [3, 4]
    source_data = pd.read_excel(file.filepath,
                                header=header,
                                skipfooter=0,
                                sheet_name=sheetnames, engine=engine)

    long_df = pd.DataFrame()
    for sheet in sheetnames:
        source_data[sheet].dropna(axis='columns', how='all', inplace=True)
        source_data[sheet].dropna(axis='index', thresh=7, inplace=True)
        id_vars = [source_data[sheet].columns[0]]
        value_vars = list(source_data[sheet].columns[1:])

        if file.year > 2013:
            melted = source_data[sheet].melt(id_vars=id_vars,
                                             value_vars=value_vars,
                                             var_name=['category_type', 'category_value'],
                                             value_name='counts')

        else:
            melted = source_data[sheet].melt(id_vars=id_vars,
                                             value_vars=value_vars,
                                             var_name=['function', 'gender'],
                                             value_name='counts')
            melted['category_type'] = [('function', 'gender')] * len(melted)
            melted['category_value'] = melted[['function', 'gender']].apply(list, axis='columns')

        melted['count_type'] = [{sheetnames[0]: 'fte',
                                 sheetnames[1]: 'headcount_full_time_and_fractional'}[sheet]] * len(melted)
        long_df = long_df.append(melted)
        long_df['lower_name'] = long_df.iloc[:, 0].str.lower()
        long_df.lower_name = long_df.lower_name.str.replace('the ', '')
        long_df.lower_name = long_df.lower_name.str.replace(',', '')
        out_df = pd.DataFrame(dict(year=[file.year] * len(long_df),
                                   source_institution_id=long_df.lower_name,
                                   source_institution_name=long_df.iloc[:, 0],
                                   source=[file.source] * len(long_df),
                                   source_category_type=long_df.category_type,
                                   source_category_name=long_df.category_value,
                                   count=long_df['counts']))

        return out_df


def ingest_uk_hesa(file: DataFile):
    if file.filepath.suffix == '.xlsx':
        engine = 'openpyxl'
    else:
        engine = None
    if file.year > 2014:
        with open(file.filepath) as f:
            skiprows = 0
            row = f.readline()
            while not row.startswith('UKPRN'):
                row = f.readline()
                skiprows = skiprows + 1

        source_data = pd.read_csv(file.filepath, skiprows=skiprows,
                                  dtype={'UKPRN': str})
        source_data.drop(columns=['Country of HE provider', 'Region of HE provider'],
                         inplace=True,
                         errors='ignore')
        source_data.drop_duplicates(inplace=True)
        cleaned_column_names = {k: k.lower().replace(' ', '_') for k in source_data.columns}
        source_data.rename(columns=cleaned_column_names, inplace=True)

        category_types = ['terms_of_employment', 'contract_levels', 'atypical_marker', 'contract_marker', 'category']
        category_values = np.column_stack((source_data[c] for c in category_types))
        category_types.remove('category')
        temp_category_types = [category_types[:]] * len(source_data)
        source_category_types = [category_markers + [category_type.lower()] for
                                 category_markers, category_type in
                                 zip(temp_category_types, source_data.category_marker)]

        category_types = ['terms_of_employment', 'contract_levels', 'atypical_marker', 'contract_marker', 'category']
        category_values = np.column_stack((source_data[c] for c in category_types)).tolist()
        category_types.remove('category')
        temp_category_types = [category_types[:]] * len(source_data)
        source_category_types = [category_markers + [category_type.lower()] for
                                 category_markers, category_type in
                                 zip(temp_category_types, source_data.category_marker)]

        out_df = pd.DataFrame(dict(year=source_data.academic_year,
                                   source_institution_id=source_data.ukprn,
                                   source_institution_name=source_data.he_provider,
                                   source=[file.source] * len(source_data),
                                   source_category_type=source_category_types,
                                   source_category_value=category_values,
                                   count=source_data.number))

    elif file.year > 2009:
        skiprows = None
        header = [8, 9]
        source_data = pd.read_excel(file.filepath, skiprows=skiprows, header=header,
                                    engine=engine,
                                    dtype={'UKPRN': str})

        source_data.drop(columns=['Country of HE provider', 'Region of HE provider'],
                         inplace=True,
                         errors='ignore')
        source_data.dropna(axis='index', inplace=True)
        id_vars = [source_data.columns[1], source_data.columns[3]]
        value_vars = list(source_data.columns[4:])

        melted = source_data.melt(id_vars=id_vars,
                                  value_vars=value_vars,
                                  var_name=['category_type', 'category_value'],
                                  value_name='counts')
        atypical_marker = file.table.split('_')[1]
        melted['atypical_marker'] = [atypical_marker] * len(melted)
        source_category_types = melted.category_type.apply(lambda x: ['atypical_marker', x.lower()])
        source_category_values = melted.category_value.apply(lambda x: [atypical_marker, x.lower()])

        out_df = pd.DataFrame(dict(year=[file.year] * len(melted),
                               source_institution_id= melted.iloc[:,0].astype(int).astype(str) ,
                               source_institution_name=melted.iloc[:,1],
                               source=[file.source] * len(melted),
                               source_category_type=source_category_types,
                               source_category_value=source_category_values,
                               count=melted['counts']))

    return out_df


def ingest_sa_hemis(file: DataFile):
    if file.filepath.suffix == '.xlsx':
        engine = 'openpyxl'
    else:
        engine = None

    table_number = file.table[0:3]
    if table_number not in ['3.3', '3.5']:
        return

    if file.year < 2010:
        if table_number == '3.3':
            header = [6, 7]
            skiprows = [8]
        elif table_number == '3.5':
            header = [4, 5]
            skiprows = [6, 7]

    elif file.year >= 2010:
        header = [6, 7]
        skiprows = [8]
    source_sheets = pd.read_excel(file.filepath,
                                  header=header,
                                  skiprows=skiprows,
                                  sheet_name=None,
                                  engine=engine)

    long_df = pd.DataFrame()
    for sheet_name in source_sheets.keys():
        org_df = source_sheets.get(sheet_name)
        org_df.dropna(axis='index', thresh=3, inplace=True)
        org_df.dropna(axis='columns', thresh=5, inplace=True)

        if table_number == '3.3':
            var_name = ['category_type', 'category_value']
            first_column_label = 'personnel_category'
        elif table_number == '3.5':
            var_name = ['rank', 'gender']
            first_column_label = 'age'

        melted = org_df.melt(id_vars=[org_df.columns[0]],
                             value_vars=[c for c in org_df.columns[1:]],
                             var_name=var_name,
                             value_name='counts')

        melted.rename(columns={org_df.columns[0]: first_column_label}, inplace=True)
        melted['source_institution_id'] = [sheet_name.lower()] * len(melted)
        long_df = long_df.append(melted)

    if table_number == '3.3':
        long_df.replace({'category_type': {'Unnamed: 1_level_0': 'RACE',
                                           'Unnamed: 2_level_0': 'RACE'}}, inplace=True)
        source_category_types = long_df.category_type.apply(lambda x: ['personnel_category', x])
        source_category_values = long_df[['personnel_category', 'category_value']].apply(list, axis='columns')

    elif table_number == '3.5':
        source_category_types = ['age', 'rank', 'gender']
        source_category_values = long_df[source_category_types].apply(list, axis='columns')
        source_category_types = [source_category_types] * len(long_df)

    out_df = pd.DataFrame(dict(year=[file.year] * len(long_df),
                               source_institution_id=long_df.source_institution_id,
                               source_institution_name=[None] * len(long_df),
                               source=[file.source] * len(long_df),
                               source_category_type=source_category_types,
                               source_category_value=source_category_values,
                               count=long_df['counts'])
                          )
    return out_df


if __name__ == '__main__':
    w = Walker('data')
    for datafile in w.walk(regexes=REGEXES):
        # if datafile.source == 'us_ipeds':
        #     print(datafile.filename, datafile.year)
        #     print(ingest_us_ipeds(datafile))
        # if datafile.source == 'au_det':
        #     print(ingest_au_det(datafile))
        # if datafile.source == 'au_indigenous':
        #     print(ingest_au_indigenous(datafile))
        if datafile.source == 'uk_hesa':
            df = ingest_uk_hesa(datafile)
            print(df)
        # if datafile.source == 'sa_hemis':
        #     print(datafile.year, datafile.table)
        #     df = ingest_sa_hemis(datafile)
        #     if df is not None:
        #         print(df.columns)
        #         print(df)
        #     else:
        #         print('skipped')
        pass
