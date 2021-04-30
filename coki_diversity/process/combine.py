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

import logging
import pandas as pd
import numpy as np
from typing import Union, Tuple, List, Dict, NamedTuple, Optional
from pathlib import Path


def load_files(dir: Union[str, Path]) -> pd.DataFrame:
    dir = Path(dir)

    df = pd.DataFrame()
    audf = pd.read_csv(dir / 'au_det__all_.csv')
    au_ind = pd.DataFrame()
    for f in dir.glob('au_indigenous*.csv'):
        au_ind = au_ind.append(pd.read_csv(f), ignore_index=True)

    audf = audf.merge(au_ind[['id', 'year', 'academic_indigenous_count', 'academic_indigenous_women_count']],
                      on=['id', 'year'],
                      how='outer')

    for f in dir.glob('*.csv'):
        logging.debug(f'Loading file {f}')
        if f.name.startswith('au'):
            continue
        else:
            df = df.append(pd.read_csv(f), ignore_index=True)

    df = df.append(audf)

    return df


def calculate_percentage(df: pd.DataFrame,
                         numerators: Union[str, List[str]],
                         denominator: str,
                         colname_modifier: 'str' = '_pc_totac',
                         decimals: Optional[int] = 2,
                         inplace: bool = False) -> Union[pd.DataFrame, None]:
    idf = df.copy(deep=True)
    zeros = np.isin(idf.academic_total_count, 0)
    idf = idf.iloc[~zeros]
    for col in numerators:
        new_col = np.round(((idf[col].divide(idf[denominator])) * 100), decimals=decimals)
        new_col_name = col + colname_modifier
        idf[new_col_name] = new_col

    if inplace:
        df = idf
        return None
    else:
        return idf
