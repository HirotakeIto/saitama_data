# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot
from snapshottest.file import FileSnapshot


snapshots = Snapshot()

snapshots['test_correspondence correspondence'] = FileSnapshot('snap_test/test_correspondence correspondence.csv')

snapshots['test_school_qestion_info school_qestion_info'] = FileSnapshot('snap_test/test_school_qestion_info school_qestion_info.csv')

snapshots['test_city city'] = FileSnapshot('snap_test/test_city city.csv')
