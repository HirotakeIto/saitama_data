import pandas as pd
from saitama_data.datasetup.models.info.classid_schoolid import SchoolClass, SchidSchoolid
from saitama_data.datasetup.models.info.classid_schoolid.model import ClassIdSchoolId


def seed():
    sc = SchoolClass().read()
    ss = SchidSchoolid().read()
    class_id_school_id = pd.merge(sc.data, ss.data, on='sch_id', how='left')
    class_id_school_id = class_id_school_id[['class_id', 'school_id', 'city_id']]
    cs = ClassIdSchoolId(class_id_school_id)
    cs.validate()
    cs.save()