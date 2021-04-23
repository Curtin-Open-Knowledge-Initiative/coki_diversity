from pathlib import Path
import os
import re
from typing import Optional, Union

REGEXES = {
    'sa_hemis': {
        'regex': '^([\w\d\s_]+\/)+(?P<year>19|20\d{2})( SA)* Staff tables[\w\s\d()]*\/(SA_)*\d{4}[\w\s\d_]*(?P<table>\d.\d[\w\s\d_-]*).xls(x)?$'
    },
    'au_det': {
        'regex': '^([\w\d\s_]+\/)+au_(?P<table>(fte|hc)_gender_level)_(?P<year>2001(_|-)2020).xlsx$'
    },
    'au_indigenous': {
        'regex': '^([\w\d\s_]+\/)+(?P<year>19|20\d{2})_staff_(?P<table>[\w\s_]*indigenous[\w\s_]*).xls(x)?$'
    },
    'uk_hesa': {
        'regex': '^([\w\d\s_]+\/)+UK( staff raw |_Table_)(?P<table>[\w\s_]*)(?P<year>19|20\d{2}-\d{2}).(csv|xlsx)$'
    },
    'us_ipeds': {
        'regex': '^([\w\d\s_]+\/)+IPEDS[\w\s_]+(?P<table>occupation_gender_race)_(?P<year>19|20\d{2}).(csv|xlsx)$'
    }
}


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


class Walker:
    data_folder = Path('data')

    def __init__(self,
                 directory,
                 regexes=REGEXES,
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


if __name__ == '__main__':
    w = Walker('data')
    for x in w.walk(regexes=REGEXES, verbose=True):
        continue
