import sys

from constants import COMMA
from executor import execute

if __name__ == "__main__":
    config=list(sys.argv[1].split(COMMA))
    application_conf_file_path = config[0]
    comparator_config_file_path = config[1]
    execute(application_conf_file_path, comparator_config_file_path)
