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

    relevant_filefilters = [f for f in filters.filefilters
                            if (
                                    (f.source in sources) and
                                    (bool(set(f.years).intersection(years)))
                            )
                            ]

    if bool(relevant_filefilters):
        relevant_filters = CategoryFilter(name=filters.name,
                                          filefilters=relevant_filefilters
                                          )
        logging.info(f'Selected the following relevant filters for {filters.name}')
        logging.info([f for f in relevant_filters.filefilters])
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
        all([
            True in
            (np.isin(row['source_category_type'], k) & np.isin(row['source_category_value'], v))
            for k, v in filter.reqs.items()
        ]
        )
        for filter in filters.filefilters
    ])
    # for filter in filters.filefilters:
    # if (row['source'] == filter.source) and (row['year'] in filter.years):
    #
    #     if all([
    #         True in
    #         (np.isin(row['source_category_type'], k) & np.isin(row['source_category_value'], v))
    #         for k, v in filter.reqs.items()]):
    #         return True
    #
    #     else:
    #         return False

    # for k, v in filter.reqs.items():
    #     type_match = np.isin(row['source_category_type'], k)
    #     value_match = np.isin(row['source_category_value'], v)
    #     test = type_match & value_match
    #     if True in type_match & value_match:
    #         continue
    #     else:
    #         return False
    # return True

    # row_test = any(
    #     [
    #         (row['source'] == filter.source) &
    #         (row['year'] in filter.years) &
    #         all(
    #             [(np.where(np.asarray(row['source_category_type']) == k)) ==
    #              (np.where(np.isin(row['source_category_value'], v))) for k, v in filter.reqs.items()]
    #         )
    #         for filter in filters]
    # )
    #
    # return row_test


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
