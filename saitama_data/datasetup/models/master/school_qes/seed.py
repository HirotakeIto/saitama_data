import pandas as pd
from saitama_data.datasetup.models.master.school_qes._seed.seed_2015_2019 import seed as seed_2015_2019
from saitama_data.datasetup.models.master.school_qes._seed.seed_2020 import seed as seed_2020
from saitama_data.datasetup.models.master import SchoolQes


def seed(save_dry=False):
    def save(data, dry_save=False):
        model = SchoolQes(data).adjust_schema().convert()
        model.validate_convert()
        if dry_save is False:
            model.save()
        # print('NUMBER OF DUPLICATED ID: ', data.duplicated(subset=['grade', 'year', 'id'], keep=False).sum())

    df = (
        pd.DataFrame()
        .append(seed_2015_2019())
        .append(seed_2020())
    )
    save(df, dry_save=save_dry)
