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
    source_count_type = file.table.split('_')[0]
    out_df = pd.DataFrame(dict(year=long_df.year,
                               source_institution_id=long_df.lower_name,
                               source_institution_name=long_df.source_name,
                               source=[file.source] * len(long_df),
                               source_category_type=[category_types] * len(long_df),
                               source_category_value=category_values,
                               count=long_df['counts'],
                               source_count_type=[source_count_type] * len(long_df)))

    return out_df
