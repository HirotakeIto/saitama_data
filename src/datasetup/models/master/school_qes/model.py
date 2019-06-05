import pandas as pd
import re
from src.datasetup.models.master.school_qes.schema import SchoolQesSchema
from src.datasetup.models.mix_in.io_mixin import CsvIOMixin


class SchoolQes(CsvIOMixin, SchoolQesSchema):
    """
    s = SchoolQes()
    s.fetch(['s51'])
    """
    path = './data/db/master/school_qes.csv'