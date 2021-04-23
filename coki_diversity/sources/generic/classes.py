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

from typing import Union, Optional, NamedTuple, Tuple, List, Dict
from pathlib import Path


class DataFile:

    def __init__(self,
                 year: int,
                 table: str,
                 filepath: Union[str, Path],
                 directory: Optional[Union[str, Path]] = None,
                 filename: Optional[str] = None,
                 source: Optional[str] = None,
                 provenance: Optional[dict] = None):
        self.table = table
        self.filepath = filepath
        self.filename = filename
        self.directory = directory
        self.source = source
        self.provenance = provenance

        if source == 'au_det':
            self.year = '_all_'
        elif '-' in year:
            self.year = int(year[0:2] + year[5:7])
            self.reporting_year_type = 'nh_academic'
        else:
            self.year = int(year)


class FileFilter(NamedTuple):
    source: str
    years: Union[Tuple[int, int], List[int]]
    reqs: Dict[str, List[str]]
    count_type: Optional[Union[str, None]] = None

    def expand_years(self):
        if type(self.years) == list:
            pass
        elif type(self.years) == tuple:
            yrs = range(*self.years)
            self._replace(years=yrs)

    def __repr__(self):
        return f"""
FileFilter for source:{self.source} years:{self.years}
    Requirements: {self.reqs}"""


class CategoryFilter:

    def __init__(self,
                 name: str,
                 filefilters: List[FileFilter]):
        self.filefilters = filefilters
        self.source = filefilters[0].source
        for i, _ in enumerate(filefilters):
            assert (self.source == filefilters[i].source)
        self.name = name


class Walker:
    data_folder = Path('data')

    def __init__(self,
                 directory,
                 regexes,
                 verbose=False):
        self.directory = Path(directory)
        self.regexes = regexes
        self.ingestor = None

    def walk(self,
             regexes=None,
             verbose=False):

        if not regexes:
            regexes = self.regexes

        for dir, directories, filenames in os.walk(self.directory):
            for filename in filenames:
                year = None
                table = None
                source = None

                filepath = Path(dir) / Path(filename)
                if verbose:
                    print(f'Matching {filepath}')
                for source_name in regexes.keys():
                    regex = re.compile(regexes[source_name].get('regex'), re.IGNORECASE)
                    match = regex.search(str(filepath))
                    if match:
                        source = source_name
                        year = match.group('year')
                        table = match.group('table')
                        if verbose:
                            print(f'Matched to: {source, year, table}')
                        break

                if year and table and source:
                    yield DataFile(year, table, filepath, dir, filename, source)
                    continue
                else:
                    print(f'\nFilepath: {filepath}\n...did not match')
