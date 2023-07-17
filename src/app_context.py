import sys

from src.utils import read_file


class AppContext:
    def __init__(self, run_time_params):
        self.run_time_params_dict = {}
        self.run_time_params = run_time_params
        self.ruleset_conf = None
        self.app_conf = None

    def build(self):
        run_time_params_list = list(self.run_time_params.split(','))

        for entry in run_time_params_list:
            name, value = entry.split('=', 1)
            self.run_time_params_dict[name.strip()] = value.strip()

        self.ruleset_conf = read_file(self.run_time_params_dict['rule_set_path'])
        self.app_conf = read_file(self.run_time_params_dict['app_conf'])

    def get_value(self,key):
        return self.app_conf.get(key)
