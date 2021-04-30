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


def small_numbers(cell):
    """Deal with the <5 and np issue in these tables on ingest"""

    if (cell == '< 5') or (cell == '<5'):
        return 4

    elif (cell == '< 10') or (cell == '<10'):
        return 7

    elif cell == 'np':
        return 0

    else:
        return cell

def to_list(element):
    return [element]

def ingest(file: DataFile):
    if file.year < 2008:
        return

    header = [2, 3]
    skiprows = 0
    skipfooter = 2
    sheet_map = {'1': 'fte',
                 '2': 'headcount'}

    if file.year > 2019:
        columns = 12
        institution_col = 1

    elif file.year > 2013:
        columns = 11
        institution_col = 0

    elif file.year > 2007:
        columns = 17
        institution_col = 0

    else:
        return

    if file.filepath.suffix == '.xlsx':
        engine = 'openpyxl'
    else:
        engine = None

    converters = {col: small_numbers for col in range(1, columns)}
    source_sheets = pd.read_excel(file.filepath,
                                  header=header,
                                  skiprows=skiprows,
                                  skipfooter=skipfooter,
                                  sheet_name=['1', '2'],
                                  converters=converters,
                                  engine=engine)

    long_df = pd.DataFrame()
    for sheet_name in source_sheets.keys():
        if sheet_name in ['1', '2']:
            sheet_df = source_sheets.get(sheet_name)
            sheet_df.dropna(axis='columns', thresh=3, inplace=True)
            sheet_df.dropna(axis='index', thresh=2, inplace=True)
            if file.year == 2020:
                sheet_df.drop(columns=[sheet_df.columns[0]], inplace=True)

            level_0_list = [col.lower() for col in sheet_df.columns.levels[0]]
            level_1_list = [col.lower() for col in sheet_df.columns.levels[1]]
            sheet_df.columns.set_levels(level_0_list, level=0, inplace=True)
            sheet_df.columns.set_levels(level_1_list, level=1, inplace=True)

            melted = sheet_df.melt(id_vars=[sheet_df.columns[0]],
                                 value_vars=[c for c in sheet_df.columns[1:]],
                                 var_name=['source_category_types', 'source_category_values'],
                                 value_name='counts')

            melted.rename(columns={sheet_df.columns[0]: 'source_name'}, inplace=True)
            melted['source_count_type'] = [sheet_map[sheet_name]] * len(melted)
            melted.source_category_types = melted.source_category_types.apply(to_list)
            melted.source_category_values = melted.source_category_values.apply(to_list)

            long_df = long_df.append(melted)

    long_df['lower_name'] = long_df.source_name.str.lower()
    long_df.lower_name = long_df.lower_name.str.replace('the ', '')
    long_df.lower_name = long_df.lower_name.str.replace(',', '')
    out_df = pd.DataFrame(dict(year=[file.year] * len(long_df),
                               source_institution_id=long_df.lower_name,
                               source_institution_name=long_df.lower_name,
                               source=[file.source] * len(long_df),
                               source_category_type=long_df.source_category_types,
                               source_category_value=long_df.source_category_values,
                               counts=long_df['counts'].astype(int, errors='ignore'),
                               source_count_type=long_df.source_count_type
                               )
                          )
    
    return out_df