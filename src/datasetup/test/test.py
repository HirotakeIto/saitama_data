import os
from src.datasetup.models.master import IdMaster, SeitoQes, SchoolQes, Gakuryoku
from src.datasetup.test.data_tester import DataTester, TestGroupSize, TestGroupCount, TestGroupStd, TestGroupMean
path = 'src/datasetup/test/log'


def save_valid():
    test_target_list = [Gakuryoku, IdMaster, SeitoQes]
    data_test_cls_list = [
        TestGroupMean(group_cols=['grade', 'year'], weo=10),
        TestGroupStd(group_cols=['grade', 'year']),
        TestGroupCount(group_cols=['grade', 'year']),
        TestGroupSize(group_cols=['grade', 'year']),
    ]
    for target_cls in test_target_list:
        target = target_cls().read()
        tester = DataTester(
            data=target.data,
            data_test_cls_list=data_test_cls_list,
            test_df_info_path=os.path.join(path,target. __class__.__name__)
        )
        tester.save_valid()
    test_target_list = [SchoolQes]
    data_test_cls_list = [
        TestGroupMean(group_cols=['key1', 'year_answer']),
        TestGroupStd(group_cols=['key1', 'year_answer']),
        TestGroupCount(group_cols=['key1', 'year_answer']),
        TestGroupSize(group_cols=['key1', 'year_answer']),
    ]
    for target_cls in test_target_list:
        target = target_cls().read()
        tester = DataTester(
            data=target.data,
            data_test_cls_list=data_test_cls_list,
            test_df_info_path=os.path.join(path,target. __class__.__name__)
        )
        tester.save_valid()
