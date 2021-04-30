import logging
import json
import pandas as pd
from pathlib import Path
from process.normalise import fix_years
from process.walker import Walker


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
                 'counts']]
    return out_df


def make_json(dir,
              suffix,
              outpath,
              source_modules,
              mode='a',
              client=None,
              write_local=True,
              write_gbq=False):
    dir = Path(dir)
    outpath = Path(outpath)
    logging.info(f'Loading files for conversion to JSON-nl {dir}')

    w = Walker(dir,
               source_modules,
               id_map_path='../data/id_mappings')

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
                    logging.info(f'Converting {key} from {f.source} to json-nl')
                    df['id'] = df.source_institution_id.map(id_map)
                    df = fix_years(df)
                    out_df = struct_records(df)
                    out_df.dropna(inplace=True)

                    if write_local:
                        out_df.to_json(outfile,
                                       orient='records',
                                       lines=True)
                        outfile.write('\n')


                    if write_gbq and (len(out_df) > 0):
                        rows_to_insert = out_df.to_dict(orient='records')[0:100]
                        table_id = 'coki-scratch-space.staff_demographics.demographics'

                        errors = client.insert_rows_json(
                            table_id, rows_to_insert, row_ids=[None] * len(rows_to_insert)
                        )  # Make an API request.
                        if errors == []:
                            print(f"New rows have been added for {f.source} {f.year}")
                        else:
                            print("Encountered errors while inserting rows: {}".format(errors))




if __name__ == '__main__':
    dir = '../../tests/fixtures/utils/test_data'
    suffix = '.hd5'
    source_modules = ['au_det', 'sa_hemis', 'uk_hesa', 'us_ipeds']
    project = 'coki-scratch-space'
    table = 'test.diversity'

    client = bigquery.Client()

    make_json(dir=dir,
              suffix=suffix,
              source_modules=source_modules,
              outpath='../../tests/fixtures/utils/test_json.json',
              client=client)
