from pandas import DataFrame
from typing import Callable, List
from snapshottest.pytest import PyTestSnapshotTest
from tests.model.lib import helper_descriptive_stats_groupby, helper_filesnapshot, helper_descriptive_stats
from saitama_data.datasetup.models.info import ClassIdSchoolId
from saitama_data.datasetup.models.info.classid_schoolid import SchidSchoolid, SchoolClass


def test_class_id_school_id(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        ClassIdSchoolId().read().data
        .pipe(helper_descriptive_stats, funcs=['count'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="class_id_school_id")


def test_sch_id_school_id(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        SchidSchoolid().read().data
        .pipe(helper_descriptive_stats_groupby, keys = ['sch_id'], funcs=['count'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="sch_id_school_id")


def test_school_class(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        SchoolClass().read().data
        .pipe(helper_descriptive_stats_groupby, keys = ['sch_id'], funcs=['count'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="school_class")

