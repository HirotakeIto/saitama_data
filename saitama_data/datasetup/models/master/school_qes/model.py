import pandas as pd
import re
from saitama_data.datasetup.models.master.school_qes.schema import SchoolQesSchema
from saitama_data.datasetup.models.mix_in.io_mixin import CsvIOMixin


class SchoolQes(CsvIOMixin, SchoolQesSchema):
    """
    s = SchoolQes()
    s.fetch(['s51'])
    """
    path = './data/db/master/school_qes.csv'
