from collections import namedtuple
from dftest.info import InfoSeries, InfosDataFrame
from pandas import isnull
import pprint
from tqdm import tqdm

printer = pprint.PrettyPrinter(indent=4)


class InfoValidation:
    """
    本当はこれもInfosDataFrameValidationだけじゃない方が拡張性高いよなあ。。。
    InfoDataframeってInfoの集合なわけで、
    """

    def __init__(self, src_is: InfoSeries, tar_is: InfoSeries):
        self.src = src_is
        self.tar = tar_is

    def test(self, **argv):
        return self._test(src_info = self.src, tar_info = self.tar, **argv)

    def _test(self, src_info: InfoSeries, tar_info: InfoSeries, is_print=False):
        result = self.is_equal(val_src=src_info.test_value, val_tar=tar_info.test_value)
        if result.result is False:
            # import pdb;pdb.set_trace()
            error_messeage = ': '.join([ str(x) for x in [result.message, 'Source::', src_info.message, 'Target::', tar_info.message]])
            # printer.pprint(error_messeage)
            if is_print == True:
                print(error_messeage)
            return (False, error_messeage)
        else:
            return (True, 'test passed')

    @staticmethod
    def is_equal(val_src, val_tar, tol = 0.0001):
        """この関数はis_equalに返すだけなんだから、TrueとFalseとかだけ返すようにすべき"""
        TestResult = namedtuple('TestResult', ('result', 'message'))  # この辺汚いな。。。Test result
        if type(val_src) != type(val_tar):
            return TestResult(False, 'cls mismatch')
        elif (type(val_src) == str) | (type(val_tar) == str):
            return TestResult(True, None) if val_src == val_tar else TestResult(False, 'str value mismatch')
        else:
            if isnull(val_src) & isnull(val_tar):
                return TestResult(True, None)
            else:
                return TestResult(True, None) if abs(val_src - val_tar) < tol else TestResult(False, 'numeric value mismatch')

    @staticmethod # infosに入れるべき
    def have_name(idf: InfosDataFrame, name: str):
        return idf.have_name(name)


def test_infos(src_idf: InfosDataFrame, tar_idf: InfosDataFrame, check_missing_src = True, check_missing_tar = True):
    messeages = []
    for src_is in tqdm(src_idf.infos):
        if tar_idf.have_name(src_is.test_name):
            tar_is = tar_idf.get_info_from_name(src_is.test_name)
            iv = InfoValidation(src_is=src_is, tar_is=tar_is)
            is_no_error, messeage = iv.test()
            if is_no_error is False:
                messeages.append(messeage)
    src_names = src_idf.names
    tar_names = tar_idf.names
    if check_missing_src is True:
        for name in set(src_names).difference(set(tar_names)):
            messeage = 'Key Error: ' + name + ' is missing from target infos'
            messeages.append(messeage)
    if check_missing_tar is True:
        for name in set(tar_names).difference(set(src_names)):
            messeage = 'Key Error: ' + name + ' is missing from source infos'
            messeages.append(messeage)
    print('\n'.join(messeages))
