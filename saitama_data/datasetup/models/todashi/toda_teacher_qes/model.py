from saitama_data.datasetup.models.todashi.toda_teacher_qes.schema import TodaTeacherQesSchema
from saitama_data.datasetup.models.mix_in.io_mixin import CsvIOMixin


class TodaTeacherQes(CsvIOMixin, TodaTeacherQesSchema):
    path = './data/db/todashi/toda_teacher_qes.csv'

    def drop_duplicated_id(self):
        self.data = (
            self.data
            .assign(count_question = lambda dfx: dfx.count(axis=1))
            .sort_values('count_question', ascending=False)
            .drop_duplicates(subset=['year', 'teacher_id'], keep='last')
            .drop('count_question', axis=1)
        )
        return self
