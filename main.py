import pandas as pd
import numpy as np
import logging
import json
from pathlib import Path
from walk import Walker, REGEXES
from ingestors import (ingest_uk_hesa,
                       ingest_sa_hemis,
                       ingest_au_indigenous,
                       ingest_au_det,
                       ingest_us_ipeds)
from normalise import *


def map_sources(
        ingestors=[ingest_uk_hesa, ingest_sa_hemis, ingest_au_indigenous, ingest_au_det, ingest_us_ipeds],
        sources=REGEXES,
        id_map_path='data/id_mappings'):
    map = dict()
    id_map_path = Path(id_map_path)
    for source in REGEXES.keys():
        test = sorted(id_map_path.iterdir())  # f'**/*.json'))
        map_path = sorted(id_map_path.glob(f'**/{source[0:2]}_id_map.json'))[0]
        with open(map_path) as f:
            id_map = json.load(f)
        i = [ingestor.__name__[7:] for ingestor in ingestors].index(source)
        map.update({source:
            {
                'ingestor': ingestors.pop(i),
                'id_map': id_map}
        }
        )

    return map


logging.basicConfig(filename='ingest.log', level=logging.DEBUG)
w = Walker('data')
mappings = map_sources()
df = pd.DataFrame()
for datafile in w.walk():
    if (datafile.source in ['us_ipeds']) and (datafile.year > 2015):
        print(datafile.filename)
        logging.info(f'Loading {datafile.filename}')
        print(datafile.source, datafile.table)
        logging.info(f'Source: {datafile.source} Table: {datafile.table}, Year: {datafile.year}')
        ingestor = mappings.get(datafile.source)['ingestor']
        ingested = ingestor(datafile)
        if ingested is not None:
            ingested = fix_years(ingested)
            ingested['cat_vals'] = ingested[['source_category_type', 'source_category_value']].apply(
                lambda x: {c: v for c, v in zip(x.source_category_type, x.source_category_value)}, axis='columns')
            logging.info(f'...using ingestor {ingestor.__name__}')
            # id_map = mappings.get(datafile.source)['id_map']
            # ingested['id'] = ingested.source_institution_id.map(id_map)
            #
            # logging.info(f'{datafile.source}: {datafile.filename} - {ingested.id.counts()} of {len(ingested)} ids mapped')
            # non_mapped = set(ingested[ingested.id.isnull()].source_institution_id)
            # logging.warning(
            #     f'Source_ids_not mapped include {non_mapped}')
            # filters = []
            # n = normalise(ingested, filters)
            # print(n)
            # logging.info(f'Normalised using {filters}')
            # logging.info(n.head())
        else:
            logging.info(f'...not currently ingesting table {datafile.table} for {datafile.source}')

df.to_csv('data/output.csv')
