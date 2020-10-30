from saitama_data.datasetup.models.work.seito_qes_sosei.model import SeitoQesSosei
from saitama_data.datasetup.models.work.seito_qes_sosei.seito_qes_sosei_base import SeitoQesSoseiBase
from saitama_data.datasetup.models.work.seito_qes_sosei.add_value import *


def seed(save_dry=True):
    get_columns_cls = [
        Book, Cramschool, NoCramschool, TeacherAttitude, ClassAttitude,
        Zyunan, Planning, Execution, Resource, Ninti, Effort, Strategy,
        Birth, BirthMonth, Selfcontrol, Selfefficacy, Dilligence,
        AlKokugo, AlMath, AlEng, TradKokugo, TradMath
    ] 
    sqsb = SeitoQesSoseiBase()
    sqsb.fetch_columns(sq_col=['id', 'grade', 'year'], sosei_col=get_columns_cls).read()
    gs = SeitoQesSosei(sqsb.data)
    gs.validate()
    if save_dry is False:
        gs.save()


