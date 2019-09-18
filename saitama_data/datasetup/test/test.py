import os
import json
from dftest import InfosDataFrame, test_infos
# from saitama_data.datasetup.test.data_tester import DataTester, TestGroupSize, TestGroupCount, TestGroupStd, TestGroupMean
path = 'saitama_data/datasetup/test/log'
get_object_name = lambda cls: cls.__class__.__name__
get_datainfo_path = lambda name: os.path.join(path,  '{0}.csv'.format(name))
get_testinfo_path = lambda name: os.path.join(path, '{0}'.format(name))


def save(model, func_list, func_list_groupby, key_groupby):
    aa = InfosDataFrame.read_from_df(
        model.data, func_list=func_list, func_list_groupby=func_list_groupby, key_groupby=key_groupby
    )
    model_name = get_object_name(model)
    aa.to_csv(get_datainfo_path(model_name), index=False)
    with open(get_testinfo_path(model_name), mode='w', encoding='utf8') as f:
        json.dump({
            'model_name': model_name,
            'save_path': get_datainfo_path(model_name),
            'args': {
                'func_list': func_list,
                'func_list_groupby': func_list_groupby,
                'key_groupby': key_groupby
            }
        }, f, ensure_ascii=False)


def valid(model, **argv):
    model_name = get_object_name(model)
    with open(get_testinfo_path(model_name), mode='r', encoding='utf8') as f:
        res = json.load(f)
    aa = InfosDataFrame.read_from_df(model.data, **res['args'])
    bb = InfosDataFrame.read_from_csv(res['save_path'])
    test_infos(aa, bb, **argv)


def get_info_save(cls):
    if cls.__name__ in ['Gakuryoku', 'IdMaster', 'SeitoQes']:
        func_list = ['mean', 'std', 'count']
        key_groupby = ['grade', 'year']
        func_list_groupby = ['mean', 'std', 'count']
    elif cls.__name__ in ['SchoolQes']:  # SchoolQesは文字列だらけでmeanとか計算できない
        func_list = ['count']
        key_groupby = ['key1', 'year_answer']
        func_list_groupby = ['count']
    elif cls.__name__ in ['SchidSchoolid', 'SchoolClass']:
        func_list = ['count']
        key_groupby = ['sch_id']
        func_list_groupby = ['mean']
    else:
        func_list = ['count']
        key_groupby = []
        func_list_groupby = []
    save_func = lambda model: save(
        model=model, func_list=func_list, key_groupby=key_groupby, func_list_groupby=func_list_groupby
    )
    valid_func = lambda model, **argv: valid(model=model, **argv)
    return save_func, valid_func

def valid_all():
    from saitama_data.datasetup.models.master import IdMaster, SeitoQes, SchoolQes, Gakuryoku
    from saitama_data.datasetup.models.info.classid_schoolid import ClassIdSchoolId, SchidSchoolid, SchoolClass
    test_target_list = [Gakuryoku, IdMaster, SchoolQes]
    for target_cls in test_target_list:
        target = target_cls().read()
        _, valid1 = get_info_save(target_cls)
        valid1(target)
        del target
        import gc;gc.collect()

    test_target_list = [ClassIdSchoolId, SchidSchoolid, SchoolClass]
    for target_cls in test_target_list:
        target = target_cls().read()
        _, valid1 = get_info_save(target_cls)
        valid1(target)
        del target
        import gc;
        gc.collect()

    gc.collect();gc.collect()
    test_target_list = [SeitoQes]
    for target_cls in test_target_list:
        target = target_cls().read()
        _, valid1 = get_info_save(target_cls)
        valid1(target)
        del target
        import gc;gc.collect()


def save_valid():
    from saitama_data.datasetup.models.master import IdMaster, SeitoQes, SchoolQes, Gakuryoku
    from saitama_data.datasetup.models.info.classid_schoolid import ClassIdSchoolId, SchidSchoolid, SchoolClass
    test_target_list = [Gakuryoku, IdMaster, SchoolQes]
    # test_target_list = [IdMaster]
    for target_cls in test_target_list:
        target = target_cls().read()
        save1, valid1 = get_info_save(target_cls)
        save1(target)
        valid1(target)
        del target
        import gc;gc.collect()

    test_target_list = [ClassIdSchoolId, SchidSchoolid, SchoolClass]
    for target_cls in test_target_list:
        target = target_cls().read()
        save1, valid1 = get_info_save(target_cls)
        save1(target)
        valid1(target)
        del target
        import gc;
        gc.collect()

    gc.collect();gc.collect()
    test_target_list = [SeitoQes]
    for target_cls in test_target_list:
        target = target_cls().read()
        save1, valid1 = get_info_save(target_cls)
        save1(target)
        valid1(target)
        del target
        import gc;gc.collect()

    #  check
    # aa = IdMaster().limit(121).read()
    # aa.mean()
    # aa.agg(['mean'])
    # aa.mst_id.mean(numeric_only=True)
    # aa.mean(numeric_only=True)
    # aa.describe()
    # save1, valid1 = get_info_save(IdMaster)
    # save1(aa)

