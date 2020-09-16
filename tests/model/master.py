from pandas import DataFrame
from typing import Callable, List
from snapshottest.pytest import PyTestSnapshotTest
from tests.model.lib import helper_descriptive_stats_groupby, helper_filesnapshot
from saitama_data.datasetup.models import IdMaster, Gakuryoku, SchoolQes, SeitoQes


def test_idmaster(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        IdMaster().read().data
        .pipe(helper_descriptive_stats_groupby, keys=['year', 'grade'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="idmaster")


def test_gakuryoku(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        Gakuryoku().read().data
        .pipe(helper_descriptive_stats_groupby, keys=['year', 'grade'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="gakuryoku")


def test_seito_qes(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        SeitoQes().read().data
        .pipe(helper_descriptive_stats_groupby, keys=['year', 'grade'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="seito_qes")


def test_schoolqes(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        SchoolQes().read().data
        .pipe(helper_descriptive_stats_groupby, keys=['key1', 'year_answer'], funcs=['count'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="schoolqes")
