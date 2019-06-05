class BaseIOMixIn:
    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        self._data = data

    def read(self, **argv):
        raise NotImplementedError('please override')

    def save(self, **argv):
        raise NotImplementedError('please override')

    # def update(self):
    #     pass

# class A:
#     def __init__(self, *args, **kwargs):
#         # super(A, self).__init__(*args, **kwargs)
#         print('A')
#
#     def wei(self):
#         print('A')
#
# class B:
#     def __init__(self, *args, **kwargs):
#         # super(B, self).__init__(*args, **kwargs)
#         print('B')
#
#     def wei(self):
#         print('B')
#
# class C:
#     def __init__(self, *args, **kwargs):
#         # super(C, self).__init__(*args, **kwargs)
#         print('C')
#
#     def wei(self):
#         print('C')
#
#
# class ABC(A, B, C):
#     def __init__(self):
#         super().__init__()
#
# abc = ABC()
# # abc.wei()

