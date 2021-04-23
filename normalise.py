import pandas as pd
import numpy as np
from typing import Union, Tuple, List, Dict, NamedTuple
from walk import DataFile
from collections import namedtuple


class CategoryFilter(NamedTuple):
    source: str
    years: Union[Tuple[int, int], List[int]]
    reqs: Dict[str, List[str]]

    def expand_years(self):
        if type(self.years) == list:
            pass
        elif type(self.years) == tuple:
            yrs = range(*self.years)
            self._replace(years=yrs)


uk_hesa_women_academic = CategoryFilter(source='uk_hesa',
                                        years=range(2015, 2020),
                                        reqs={
                                            'contract_levels': ['All'],
                                            'terms_of_employment': ['All'],
                                            'contract_marker': ['Academic'],
                                            'sex': ['Female'],
                                        })

uk_hesa_women_academic_09_12 = CategoryFilter(source='uk_hesa',
                                              years=range(2009, 2015),
                                              reqs={
                                                  'atypical_marker': ['academic excl atypical', 'academic atypical'],
                                                  'gender': ['female'],
                                              })

uk_hesa_women_academic_13_14 = CategoryFilter(source='uk_hesa',
                                              years=range(2009, 2015),
                                              reqs={
                                                  'atypical_marker': ['academic excl atypical', 'academic atypical'],
                                                  'sex': ['female'],
                                              })

au_det_women_academic = CategoryFilter(source='au_det',
                                       years=range(2000, 2021),
                                       reqs={
                                           'current_duties_classification': ['Above Senior Lecturer',
                                                                             'Senior lecturer (Level C)',
                                                                             'Lecturer (Level B)',
                                                                             'Below lecturer (Level A)'
                                                                             ],
                                           'gender': ['Female']
                                       })


def normalise(df: pd.DataFrame,
              filters: List[CategoryFilter],
              **kwargs):
    filtered = filter_df(df, filters)
    output_series = filtered.groupby(['id', 'year', 'source', 'source_institution_name'])['counts'].agg('sum')
    return output_series


def filter_row(row,
               filters,
               **kwargs):
    row_test = any(
        [
            (row['source'] == filter.source) &
            (row['year'] in filter.years) &
            all(
                [(np.where(row['source_category_type'] == k)) ==
                 (np.where(row['source_category_value'] == v)) for k, v in filter.reqs.items()]
            )
            for filter in filters]
    )

    return row_test


def filter_df(df,
              filters,
              **kwargs):
    bool_series = df.apply(filter_row, args=[filters], kwds=kwargs, axis='columns')
    return df[bool_series]


def fix_years(df):
    if df.year.dtype == int:
        return df
    elif df.year.dtype == float:
        df.year = df.year.astype(int)
        return df
    elif len(df.year[0]) == 7:
        df.year = df.year.apply(lambda x: int(x[0:2] + x[5:7]))
        return df
    else:
        return None
