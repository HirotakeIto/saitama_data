from configparser import ConfigParser


class ReadConfig2015(object):
    def __init__(self, path):
        self.inifile = ConfigParser()
        self.read(path)

    def read(self, config_path):
        self.inifile.read(config_path, encoding='utf8')

    def get_setting(self):
        inifile = self.inifile
        self.downpath = inifile.get('filepath', '2015_raw')
        self.info = inifile.get('filepath', 'info')
        self.workpath = inifile.get('filepath', 'work')
        self.master = inifile.get('filepath', 'master')
        return self.downpath, self.info


class ReadConfig2016(object):
    def __init__(self, path):
        self.inifile = ConfigParser()
        self.read(path)

    def read(self, config_path):
        self.inifile.read(config_path, encoding='utf8')

    def get_setting(self):
        inifile = self.inifile
        self.downpath = inifile.get('filepath', '2016_raw')
        self.info = inifile.get('filepath', 'info')
        self.workpath = inifile.get('filepath', 'work')
        self.master = inifile.get('filepath', 'master')
        return self.downpath, self.info


class ReadConfig2017(object):
    def __init__(self, path):
        self.inifile = ConfigParser()
        self.read(path)

    def read(self, config_path):
        self.inifile.read(config_path, encoding='utf8')

    def get_setting(self):
        inifile = self.inifile
        self.downpath = inifile.get('filepath', '2017_raw')
        self.info = inifile.get('filepath', 'info')
        self.workpath = inifile.get('filepath', 'work')
        self.master = inifile.get('filepath', 'master')
        return self.downpath, self.info


class ReadConfig2018(object):
    def __init__(self, path):
        self.inifile = ConfigParser()
        self.read(path)

    def read(self, config_path):
        self.inifile.read(config_path, encoding='utf8')

    def get_setting(self):
        inifile = self.inifile
        self.info = inifile.get('path2018', 'info')
        self.downpath = inifile.get('path2018', 'downpath')
        self.school_master = inifile.get('path2018', 'school_master')
        self.school_code = inifile.get('path2018', 'school_code')
        self.id_master = inifile.get('path2018', 'id_master')
        self.master = inifile.get('path2018', 'master')
        return self


class ReadConfigTodashi20162017(object):
    def __init__(self, path):
        self.inifile = ConfigParser()
        self.read(path)

    def read(self, config_path):
        self.inifile.read(config_path, encoding='utf8')

    def get_setting(self):
        inifile = self.inifile
        self.downpath = inifile.get('filepath', 'todashi')
        self.info = inifile.get('filepath', 'info')
        self.workpath = inifile.get('filepath', 'work')
        self.master = inifile.get('filepath', 'master')
        return self.downpath, self.info