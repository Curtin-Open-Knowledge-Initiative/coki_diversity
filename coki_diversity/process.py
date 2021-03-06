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
import os
import re

from google.cloud import bigquery
import pydata_google_auth
import pandas as pd
from pathlib import Path
from typing import Union, List, Optional
from types import ModuleType
from process.walker import Walker
from process.normalise import normalise, fix_years
from process.combine import load_files, calculate_percentage
from process.bigquery import make_json


def process_input_files(input_directory: Union[Path, str],
                        source_modules: Union[List[str], List[ModuleType]],
                        output_directory: Union[Path, str],
                        skip_processed: bool = True) -> None:
    input_directory = Path(input_directory)
    output_directory = Path(output_directory)

    w = Walker(input_directory,
               source_modules)

    for datafile in w.walk():
        logging.info('Processing Input Files:')
        logging.info(f'Loading {datafile.filename}')
        logging.info(f'Source: {datafile.source} Table: {datafile.table}, Year: {datafile.year}')
        filename = Path(f'{datafile.source}_{datafile.year}.hd5')
        with pd.HDFStore(output_directory / filename) as store:
            if skip_processed and (f'/{datafile.table}' in store.keys()):
                logging.info(f'...file already processed. Skipping. Set skip_processed to False to re-ingest')
                continue

            ingestor = w.mapping.get(datafile.source)['ingestor']
            logging.info(f'...ingesting file using ingestor for {datafile.source}')
            ingested = ingestor.ingest(datafile)
            if ingested is not None:
                store[datafile.table] = ingested
                logging.info(
                    f'Ingested file stored in {datafile.source}_{datafile.year}.hd5 with key {datafile.table}')


def normalise_ingested_files(ingested_directory: Union[Path, str],
                             output_directory: Union[Path, str],
                             source_modules: Union[List[str], List[ModuleType]],
                             skip_processed: bool = False) -> None:
    ingested_directory = Path(ingested_directory)
    output_directory = Path(output_directory)

    logging.info('Starting normalisation run \n\n')

    w = Walker(ingested_directory,
               source_modules=source_modules)

    for ingested_file in w.walk(stage='ingested'):

        filename = Path(f'{ingested_file.source}_{ingested_file.year}.csv')
        logging.info(f'Loading file: {filename}')
        filepath = output_directory / filename
        if filepath.is_file() and skip_processed:
            out_df = pd.read_csv(filepath)
            logging.info(f'...{filename} has been previously processed')
        else:
            out_df = pd.DataFrame()
            logging.info(f'...{filename} was not previously processed')
        temp_df = pd.DataFrame()
        filter_list = w.mapping.get(ingested_file.source)['filter_list']
        id_map = w.mapping.get(ingested_file.source)['id_map']

        with pd.HDFStore(ingested_file.filepath) as store:
            if len(store.keys()) == 0:
                continue
            for table in store.keys():
                ingested = store[table]
                ingested = fix_years(ingested)
                ingested['id'] = ingested.source_institution_id.map(id_map)
                temp_df = temp_df.append(ingested)
            for filters in filter_list:
                if filters.name not in out_df.columns:
                    logging.info(f'...running {filters.name}')
                    out_df[filters.name] = normalise(temp_df, filters=filters)

        out_df.to_csv(filepath)


def combine_files(normalised_directory: Union[str, Path],
                  output_directory: Union[str, Path],
                  filename: Union[str, Path],
                  skip_processed: Optional[bool] = False):
    logging.info(f'Combining files in {normalised_directory}')
    normalised_directory = Path(normalised_directory)
    output_directory = Path(output_directory)
    filename = Path(filename)
    outpath = output_directory / filename
    if outpath.is_file() and skip_processed:
        return

    df = load_files(normalised_directory)
    pdf = calculate_percentage(df,
                         numerators=['academic_women_count',
                                     'academic_indigenous_count',
                                     'academic_white_count',
                                     'academic_indigenous_women_count'],
                         denominator='academic_total_count')

    pdf.to_csv(output_directory / filename)


if __name__ == '__main__':
    logging.basicConfig(filename='../logs/ingest.log', level=logging.DEBUG)
    logging.info('Starting a processing run...\n\n')
    source_modules = ['au_det','au_indigenous', 'nz_moe',  'au_det', 'sa_hemis', 'uk_hesa', 'us_ipeds', ]
    # process_input_files('../data/input',
    #                     source_modules=source_modules,
    #                     output_directory='../data/ingested',
    #                     skip_processed=False)
    #
    # normalise_ingested_files('../data/ingested',
    #                          source_modules=source_modules,
    #                          output_directory='../data/normalised',
    #                          skip_processed=False)
    #
    # combine_files('../data/normalised',
    #               '../data/combined',
    #               filename='combined.csv',
    #               skip_processed=False)

    make_json('../data/ingested',
              suffix='.hd5',
              outpath='../data/bq_json/bq_json.json',
              source_modules=source_modules,
              write_local=True,
              write_gbq=False)
