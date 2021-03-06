from pandas import DataFrame
from typing import Callable, List
from snapshottest.pytest import PyTestSnapshotTest
from tests.model.lib import helper_descriptive_stats_groupby, helper_filesnapshot, helper_descriptive_stats
from saitama_data.datasetup.models.info import ClassIdSchoolId, City, SchoolQestionInfo
from saitama_data.datasetup.models.info.classid_schoolid import SchidSchoolid, SchoolClass
from saitama_data.datasetup.models.info.correspondence.model import Correspondence


def test_class_id_school_id(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        ClassIdSchoolId().read().data
        .pipe(helper_descriptive_stats, funcs=['count'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="class_id_school_id")


def test_sch_id_school_id(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        SchidSchoolid().read().data
        .sort_values(['sch_id'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="sch_id_school_id")


def test_school_class(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        SchoolClass().read().data
        .sort_values(['class_id'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="school_class")


def test_correspondence(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        Correspondence().read().data
        .sort_values(['qes', 'question_id', 'year'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="correspondence")


def test_school_qestion_info(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        SchoolQestionInfo().read().data
        .sort_values(['key_unique'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="school_qestion_info")


def test_city(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        City().read()
        .sort_values(['city_id'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="city")
