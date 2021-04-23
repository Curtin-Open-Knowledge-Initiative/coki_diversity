import logging
import pandas as pd
from observatory.reports.report_utils import get_gcp_credentials
from pathlib import Path
from coki_diversity.process.normalise import fix_years
from coki_diversity.process.walker import Walker

logging.basicConfig(filename='../../logs/loaders.log', level=logging.DEBUG)


def map_categories(row):
    return [{'source_category_type': type, 'source_category_value': value}
            for type, value in zip(row.source_category_type, row.source_category_value)]


def struct_records(df):
    assert 'source_category_type' in df.columns
    assert 'source_category_value' in df.columns

    df['source_categories'] = df.apply(map_categories, axis='columns')
    out_df = df[['year',
                 'id',
                 'source',
                 'source_institution_id',
                 'source_institution_name',
                 'source_categories',
                 'count']]
    return out_df


def make_json(dir,
              suffix,
              outpath,
              source_modules,
              mode='a'):
    dir = Path(dir)
    outpath = Path(outpath)
    logging.info(f'Loading files for conversion to JSON-nl {dir}')
    w = Walker(dir,
               source_modules,
               id_map_path='../../data/id_mappings')

    if ~outpath.is_file() and mode == 'a':
        outpath.touch()
    with open(outpath, mode=mode) as outfile:
        for f in w.walk(stage='ingested'):
            if f.filepath.suffix != suffix:
                continue

            id_map = w.mapping.get(f.source)['id_map']
            with pd.HDFStore(f.filepath) as store:
                for key in store.keys():
                    df = store[key]
                    logging.info(f'Converting {key} from {f} to json-nl')
                    df['id'] = df.source_institution_id.map(id_map)
                    df = fix_years(df)
                    out_df = struct_records(df)
                    out_df.to_json(outfile,
                                   orient='records',
                                   lines=True)
                    outfile.write('\n')


def load_dir_to_gbq(dir,
                    suffix,
                    project,
                    table,
                    credentials=None,
                    if_exists='append',
                    replace_table='replace'):
    dir = Path(dir)
    logging.info(f'Loading files for gbq transfer from {dir}')
    files = dir.glob(f'*.{suffix}')

    if not credentials:
        credentials = get_gcp_credentials()

    for i, f in enumerate(files):
        if suffix == 'hd5':
            store = pd.HDFStore(f)
            for key in store.keys():
                if i == 0:
                    exists = replace_table
                else:
                    exists = if_exists
                df = store[key]
                logging.info(f'Starting upload to gbq {table}')
                df.to_gbq(destination_table=table,
                          credentials=credentials,
                          project_id=project,
                          if_exists=exists)

        elif suffix == 'csv':
            df = pd.read_csv(f)
            logging.info(f'Starting upload to gbq {table}')
            df.to_gbq(destination_table=table,
                      credentials=credentials,
                      project_id=project,
                      if_exists=if_exists)


if __name__ == '__main__':
    dir = '../../tests/fixtures/utils/test_data'
    suffix = '.hd5'
    source_modules = ['au_det', 'sa_hemis', 'uk_hesa', 'us_ipeds']
    project = 'coki-scratch-space'
    table = 'test.diversity'

    # load_dir_to_gbq(dir,
    #                 suffix,
    #                 project,
    #                 table,
    #                 if_exists='fail')

    make_json(dir=dir,
              suffix=suffix,
              source_modules=source_modules,
              outpath='../../tests/fixtures/utils/test_json.json')
