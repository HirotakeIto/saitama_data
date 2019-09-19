import pandas as pd
from saitama_data.datasetup.models.external.graduation_rate.schema import GraduationRateSchema
from saitama_data.datasetup.models.info.city.model import City
from saitama_data.lib.safe_path import safe_path


def get_data():
    df = (
        City()
        .read()
        [['city_id', 'city_name']]
        .merge(
            (
                pd.read_csv('./data/other/estat/saitama_result.csv')
                    .rename(columns={'001': 'num_all', '005': 'num_graduate', 'city': 'city_name'})
                    .assign(
                    rate_graduation=lambda dfx: dfx['num_graduate'] / dfx['num_all']
                )
                [['city_name', 'rate_graduation']]
            ),
            on='city_name',
            how='left'
        )
        [['city_id', 'rate_graduation']]
    )
    # schemaだけ持ってきてvalidateすることはかなりのhackingであることは忘れなきよう
    s = GraduationRateSchema(df)
    s.convert()
    s.validate_convert()
    s.adjust_schema()
    return df