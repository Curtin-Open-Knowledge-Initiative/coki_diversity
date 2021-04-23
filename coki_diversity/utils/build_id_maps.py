import pandas as pd
import json
from pathlib import Path

MAP_FOLDER = Path('../../data/id_mappings')

"""
UK HESA Mappings

Obtained from Wikidata using the following query
"""

with open(MAP_FOLDER / 'UKPRN-GRID.json') as f:
    ukprn_grid = json.load(f)

id_map = dict()
for item in ukprn_grid:
    id_map.update({item.get('ukprn') : item.get('grid')})

with open(MAP_FOLDER / 'uk_id_map.json', 'w') as f:
    json.dump(id_map, f)

"""
US IPEDS Mappings

Obtained from Wikidata using the following query...
"""

with open(MAP_FOLDER / 'IPEDS-GRID.json') as f:
    ukprn_grid = json.load(f)

id_map = dict()
for item in ukprn_grid:
    id_map.update({item.get('ipeds') : item.get('grid')})

with open(MAP_FOLDER / 'us_id_map.json', 'w') as f:
    json.dump(id_map, f)

"""
Australian University Name Mappings

Manually curated list
"""

au_id_info = pd.read_csv(MAP_FOLDER / 'au_name_id_mappings.csv')
au_id_info['lower_name'] = au_id_info.Institution.str.lower()
au_id_info.lower_name = au_id_info.lower_name.str.replace('the ', '')

id_map = {row.lower_name: row.GRID for _, row in au_id_info.iterrows()}
with open(MAP_FOLDER / 'au_id_map.json', 'w') as f:
    json.dump(id_map, f)

"""
South African HEMIS ID to GRID mappings

Manually curated list
"""

sa_id_info = pd.read_csv(MAP_FOLDER / 'sa_name_id_mappings.csv')
sa_id_info['hemis'] = sa_id_info['Local ID'].str.lower()
id_map = {row.hemis: row.GRID for _, row in sa_id_info.iterrows()}
with open(MAP_FOLDER / 'sa_id_map.json', 'w') as f:
    json.dump(id_map, f)