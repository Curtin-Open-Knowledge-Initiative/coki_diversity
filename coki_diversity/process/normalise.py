import logging
import pandas as pd
import numpy as np
from typing import Union, Tuple, List, Dict, NamedTuple
from coki_diversity.sources.generic import FileFilter, CategoryFilter


def normalise(df: pd.DataFrame,
              filters: CategoryFilter,
              **kwargs) -> pd.Series:
    sources = df.source.unique()
    years = df.year.unique()
    if 'source_count_type' in df.columns:
        count_types = df.source_count_type.unique()
    else:
        count_types = []

    relevant_filefilters = [f for f in filters.filefilters
                            if (
                                    (f.source in sources) and
                                    (bool(set(f.years).intersection(years))) and
                                    (f.count_type in count_types if f.count_type is not None else True)
                            )
                            ]

    if bool(relevant_filefilters):
        relevant_filters = CategoryFilter(name=filters.name,
                                          filefilters=relevant_filefilters
                                          )
        logging.debug(f'Selected the following relevant filters for {filters.name}')
        logging.debug([f for f in relevant_filters.filefilters])
        filtered = filter_df(df, relevant_filters)
        output_series = filtered.groupby(['id', 'year', 'source', 'source_institution_name'])['counts'].agg('sum')

    else:
        output_series = pd.Series()

    return output_series


def filter_row(row,
               filters,
               **kwargs):
    return any([
        (row['source'] == filter.source) &
        (row['year'] in filter.years) &
        (row['source_count_type'] in filter.count_type if filter.count_type is not None else True) &
        all([
            True in
            (np.isin(row['source_category_type'], k) & np.isin(row['source_category_value'], v))
            for k, v in filter.reqs.items()
        ]
        )
        for filter in filters.filefilters
    ])


def filter_df(df,
              filters,
              **kwargs):
    bool_series = df.apply(filter_row, args=[filters], kwds=kwargs, axis='columns')
    return df[bool_series]


def fix_years(df):
    if df.year.dtype == int:
        return df
    elif df.year.dtype == float:
        df.year = df.year.astype(int, errors='ignore')
        return df
    elif len(df.year[0]) == 7:
        df.year = df.year.apply(lambda x: int(x[0:2] + x[5:7]))
        return df
    else:
        return None
