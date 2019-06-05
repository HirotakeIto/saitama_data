from .school_qes.model import SchoolQes
from .id_master.model import IdMaster
from .id_master.extention import PrimeId
from .gakuryoku.model import Gakuryoku
from .seito_qes.model import SeitoQes
# from .gakuryoku


def seed():
    from .id_master.seed import seed as id_master_seed
    from .gakuryoku.seed import seed as gakuryoku_seed
    from .seito_qes.seed import seed as seito_qes_seed
    from .school_qes.seed import seed as school_qes_seed
    id_master_seed()
    gakuryoku_seed()
    seito_qes_seed()
    school_qes_seed()
