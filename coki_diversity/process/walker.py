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
import json
import os
import re
import logging
from pathlib import Path
from importlib import import_module
from typing import Union, Optional, Dict

import coki_diversity.sources as sources
from coki_diversity.sources.generic import DataFile


class Walker:
    data_folder = Path('data')

    def __init__(self,
                 directory: Union[str, Path],
                 source_modules,
                 source_package=sources,
                 verbose=False,
                 id_map_path='../data/id_mappings'):
        self.directory = Path(directory)
        self.ingestor = None

        self.mapping = self.map_sources(source_modules,
                                        source_package,
                                        id_map_path=id_map_path)

    def walk(self,
             mapping: Optional[Union[Dict, None]] = None,
             stage: Optional[str] = 'raw',
             verbose: Optional[bool] = False):

        if stage == 'ingested':
            table = '_all_'
            mapping = self.mapping
            for source in mapping.keys():
                mapping[source].update(dict(regex=re.compile(source + '_(?P<year>(20[0-9]{2})|(_all_)).hd5')))

        if not mapping:
            mapping = self.mapping

        for dir, directories, filenames in os.walk(self.directory):
            for filename in filenames:
                year = None
                source = None
                if stage != 'ingested':
                    table = None

                filepath = Path(dir) / Path(filename)
                if verbose:
                    print(f'Matching {filepath}')
                for source_name in mapping.keys():
                    regex = mapping[source_name].get('regex')
                    match = regex.search(str(filename))
                    if match:
                        source = source_name
                        year = match.group('year')
                        if not table:
                            table = match.group('table')
                        if verbose:
                            print(f'Matched to: {source, year, table}')
                        break

                if year and table and source:
                    logging.info(f'Filepath: {filepath} matched')
                    logging.info(f'source: {source} year: {year} table: {table}')
                    yield DataFile(year, table, filepath, dir, filename, source)
                    continue

                else:
                    logging.info(f'Filepath: {filepath}...did not match any known source')

    def map_sources(self,
                    source_modules,
                    source_package=sources,
                    id_map_path='../data/id_mappings'):

        mapping = dict()
        id_map_path = Path(id_map_path)

        for module in source_modules:
            fullname = f'coki_diversity.sources.{module}'
            source_module = import_module(fullname)
            map_path = sorted(id_map_path.glob(f'**/{module[0:2]}_id_map.json'))[0]
            with open(map_path) as f:
                id_map = json.load(f)
            mapping.update({module:
                {
                    'ingestor': source_module.ingestor,
                    'id_map': id_map,
                    'regex': source_module.file_regex,
                    'filter_list': source_module.filter_list}
            }
            )

        return mapping
