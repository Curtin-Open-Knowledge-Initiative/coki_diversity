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

import numpy as np
import pandas as pd
from ..generic import DataFile


def ingest(file: DataFile):
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
                                   source_institution_id=melted.iloc[:, 0].astype(int).astype(str),
                                   source_institution_name=melted.iloc[:, 1],
                                   source=[file.source] * len(melted),
                                   source_category_type=source_category_types,
                                   source_category_value=source_category_values,
                                   counts=melted['counts']))

    return out_df
