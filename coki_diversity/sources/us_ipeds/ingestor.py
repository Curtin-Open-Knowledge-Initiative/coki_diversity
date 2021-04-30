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
from pandas.api.types import is_numeric_dtype
from ..generic import DataFile


def ingest(file: DataFile):
    """Take a provided filepath and process to generate back a semi-standardised pandas dataframe"""

    # Post 2015 files
    if file.year >= 2015:
        engine = 'openpyxl'
        source_data = pd.read_excel(file.filepath, header=4, na_values='-', engine=engine)

        cleaned_column_names = {k: k.lower().replace(' ', '_') for k in source_data.columns}
        cleaned_column_names.update(dict(unitid='unit_id'))
        source_data.rename(columns=cleaned_column_names, inplace=True)
        source_data['occupation_filled'] = source_data.occupation.ffill()
        for col in source_data.columns:
            if not is_numeric_dtype(source_data[col]):
                source_data[col] = source_data[col].str.lower()

        category_types = ['occupation_filled', 'gender', 'ethnicity']
        melt_id_vars = ['unit_id', 'institution_name', 'occupation', 'occupation_filled', 'gender']
        melt_var_name = 'ethnicity'

    elif file.year < 2015:
        if file.filepath.suffix == '.csv':
            source_data = pd.read_csv(file.filepath, header=0)
        elif file.filepath.suffix == '.xlsx':
            engine = 'openpyxl'
            source_data = pd.read_excel(file.filepath, engine=engine)
        source_data.drop(columns=['IDX_HR', 'IDX_S'], errors='ignore', inplace=True)
        for col in source_data.columns:
            if not is_numeric_dtype(source_data[col]):
                source_data[col] = source_data[col].str.lower()
        category_types = ['occupation_and_status', 'gender_and_ethnicity']
        # Split the column names to remove column prefix
        occupation_column_name = source_data.columns[3]
        cleaned_column_names = {k: k.split('.')[1].lower().replace(' ', '_') for k in source_data.columns[3:]}
        cleaned_column_names.update({'unitid': 'unit_id',
                                     'institution name': 'institution_name',
                                     occupation_column_name: 'occupation_and_status'})
        source_data.rename(columns=cleaned_column_names, inplace=True)
        melt_id_vars = ['unit_id', 'institution_name', 'year', 'occupation_and_status']
        melt_var_name = 'gender_and_ethnicity'

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
                               counts=melted['counts'].astype(int, errors='ignore'))
                          )
    if 'year' in melted.columns:
        out_df['year'] = melted.year
    else:
        out_df['year'] = [file.year] * len(melted)

    return out_df
