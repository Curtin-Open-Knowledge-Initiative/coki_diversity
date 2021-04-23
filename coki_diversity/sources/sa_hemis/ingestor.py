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

import numpy as np
import pandas as pd
from ..generic import DataFile


def ingest(file: DataFile):
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
            if file.year < 2010:
                org_df['GENDER', 'MALE'] = org_df['RACE', 'MALE']
                org_df.drop(columns=('RACE', 'MALE'), inplace=True)
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
                               source_institution_name=['not_captured'] * len(long_df),
                               source=[file.source] * len(long_df),
                               source_category_type=source_category_types,
                               source_category_value=source_category_values,
                               count=long_df['counts'])
                          )
    return out_df