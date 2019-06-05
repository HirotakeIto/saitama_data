import pandas as pd
from saitama_data.datasetup.models.info.city.schema import CitySchema

def get_data():
    path = 'data/original_data/マスタデータ/replacelist.xlsx'
    data = (
        pd.read_excel(path, '市町村教育委員会')
        .rename(columns={
            '市町村教育委員会乱数': 'city_id',
            '市町村教育委員会コード': 'city_code',
            '市町村教育委員会名': 'organization_name'
        })
        .assign(
            city_name = lambda dfx: dfx['organization_name'].str.replace('教育委員会', '')
        )
    )
    s = CitySchema(data)
    s.validate_convert()
    s.adjust_schema()
    return s.data



