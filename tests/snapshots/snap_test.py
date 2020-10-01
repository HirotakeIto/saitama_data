# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot
from snapshottest.file import FileSnapshot


snapshots = Snapshot()

snapshots['test_correspondence correspondence'] = FileSnapshot('snap_test/test_correspondence correspondence.csv')

snapshots['test_school_qestion_info school_qestion_info'] = FileSnapshot('snap_test/test_school_qestion_info school_qestion_info.csv')

snapshots['test_city city'] = FileSnapshot('snap_test/test_city city.csv')

snapshots['test_seito_qes seito_qes'] = FileSnapshot('snap_test/test_seito_qes seito_qes.csv')

snapshots['test_class_id_school_id class_id_school_id'] = FileSnapshot('snap_test/test_class_id_school_id class_id_school_id.csv')

snapshots['test_sch_id_school_id sch_id_school_id'] = FileSnapshot('snap_test/test_sch_id_school_id sch_id_school_id.csv')

snapshots['test_school_class school_class'] = FileSnapshot('snap_test/test_school_class school_class.csv')

snapshots['test_idmaster idmaster'] = FileSnapshot('snap_test/test_idmaster idmaster.csv')

snapshots['test_gakuryoku gakuryoku'] = FileSnapshot('snap_test/test_gakuryoku gakuryoku.csv')

snapshots['test_schoolqes schoolqes'] = FileSnapshot('snap_test/test_schoolqes schoolqes.csv')

snapshots['test_seito_qes_sosei seito_qes_sosei'] = FileSnapshot('snap_test/test_seito_qes_sosei seito_qes_sosei.csv')
