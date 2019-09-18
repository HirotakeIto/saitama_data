from saitama_data.datasetup.models.info import seed_school_class, seed_schid_schoolid, \
    seed_classid_schoolid, seed_correspondence, seed_school_qes_info
from saitama_data.datasetup.models.master import seito_qes_seed, gakuryoku_seed, id_master_seed, school_qes_seed



"""
ここは順番を考慮する必要あり

０からビルド
seed_info_master　→　seed_master

だけど、普通のデータ追加業務の時は
seed_master　→　seed_info_master
の方が良いと思われる（理由！）

"""

def seed_master(save_dry=True):
    # save_dry = True
    seed_correspondence(save_dry)
    id_master_seed(save_dry)
    gakuryoku_seed(save_dry)
    seito_qes_seed(save_dry)
    seed_school_qes_info()
    # school_qes_seed()

def seed_info_master(save_dry=True):
    # save_dry = True
    seed_schid_schoolid(save_dry)
    seed_school_class(save_dry)
    seed_classid_schoolid(save_dry)

