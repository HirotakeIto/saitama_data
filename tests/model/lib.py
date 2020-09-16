import os
from pandas import DataFrame
from typing import Callable, List
from snapshottest.pytest import PyTestSnapshotTest
from snapshottest.file import FileSnapshot

__folder__  = "tests/tmpdata"


def helper_filesnapshot(snapshot: PyTestSnapshotTest, dfx: DataFrame, name: str):
    path_save = os.path.join(__folder__, "{name}.csv".format(name=name))
    dfx.to_csv(path_save, index = False)
    snapshot.assert_match(FileSnapshot(path_save), name)


def helper_descriptive_stats(dfx: DataFrame, funcs: Callable or str or List = ['count', 'mean', 'std']):
    return (
        dfx
        .agg(funcs)
        .reset_index()
    )


def helper_descriptive_stats_groupby(dfx: DataFrame, keys:List[str], funcs: Callable or str or List = ['count', 'mean', 'std']):
    return (
        dfx
        .groupby(keys, sort=True)
        .agg(funcs)
        .reset_index().melt(id_vars=keys)
    )
