from saitama_data.datasetup.models.master.school_qes.model import SchoolQesAccepter


class RateSchoolAttendancesAccepter(SchoolQesAccepter):
    name = 'RateSchoolAttendancesAccepter'

    def __init__(self, merge_key=None, merge_key_rename_mapper=None, **argv):
        super().__init__(squestion=['s33', 's36'], is_numeric=True, **argv)
        self.merge_key = ['year', 'school_id', 'grade'] if merge_key is None else merge_key
        self.merge_key_rename_mapper= {
            'year_target': 'year'
        } if merge_key_rename_mapper is None else merge_key_rename_mapper
        self.data = (
            self.data
            .assign(
                rate_school_attendance=lambda dfx: (dfx['s36'] / dfx['s33']).clip(0, 1)
            )
            [['year_target', 'school_id', 'grade', 'rate_school_attendance']]
            .rename(columns = {'year_target': 'year'})
            .rename(columns = self.merge_key_rename_mapper)
        )

    def accept(self, t):
        t.data = (
            t.data
            .merge(
                self.data,
                on=self.merge_key,
                how='left'
            )
        )
        print('{accepter}:::now tabele matrix is'.format(accepter=self.name),
              t.data.shape)
