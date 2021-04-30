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

    sheet_name = 'Staff type x Ethnic x Gender'
    skiprows = 0
    header = [3,4,5]

    source_data = pd.read_excel(file.filepath,
                                  header=header,
                                  skiprows=skiprows,
                                  sheet_name=sheet_name,
                                  engine=engine)

    long_df = pd.DataFrame()

    source_data.dropna(axis='columns', thresh=5, inplace=True)
    source_data.ffill(inplace=True)

    id_vars = source_data.columns[0:3]
    melted = source_data.melt(id_vars=list(id_vars),
                              var_name=['source_count_type', 'year', 'gender'],
                              value_name='counts')
    melted = melted.applymap(lambda s: s.lower() if type(s) == str else s)

    cleaned_column_names = {melted.columns[0]: 'provider',
                            melted.columns[1]: 'staff type/group',
                            melted.columns[2]: 'ethnic group'}
    melted.rename(columns=cleaned_column_names, inplace=True)

    category_types = ['staff type/group', 'ethnic group', 'gender']
    melted['category_types'] = [category_types] * len(melted)
    melted['category_values'] = melted[category_types].apply(list, axis='columns')
    melted['source_count_type'] = melted.source_count_type.map({'fte': 'fte',
                                                                'number of staff': 'headcount'
                                                                })

    long_df = long_df.append(melted)

    out_df = pd.DataFrame(dict(year=long_df.year.astype(int, errors='ignore'),
                               source_institution_id=long_df.provider,
                               source_institution_name=long_df.provider,
                               source=[file.source] * len(long_df),
                               source_category_type=long_df.category_types,
                               source_category_value=long_df.category_values,
                               counts=long_df.counts,
                               source_count_type=long_df.source_count_type))
    return out_df
