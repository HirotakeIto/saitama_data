from pandas import DataFrame
from typing import Callable, List
from snapshottest.pytest import PyTestSnapshotTest
from tests.model.lib import helper_descriptive_stats_groupby, helper_filesnapshot
from saitama_data.datasetup.models import SeitoQesSosei


def test_seito_qes_sosei(snapshot: PyTestSnapshotTest):
    df: DataFrame = (
        SeitoQesSosei().read().data
        .pipe(helper_descriptive_stats_groupby, keys=['year', 'grade'])
    )
    helper_filesnapshot(snapshot=snapshot, dfx=df, name="seito_qes_sosei")
