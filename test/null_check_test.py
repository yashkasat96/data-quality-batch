from pathlib import Path

from executor import execute
from utils import read_file

app_conf_file_path = Path(__file__).parent / "conf/properties/application.properties"
ruleset_conf_file_path = Path(__file__).parent / "conf/json/local/employee_multiple_rules.json"

rule_set_path =  str(ruleset_conf_file_path)
app_conf = str(app_conf_file_path)
execute(f'rule_set_path={rule_set_path},app_conf={app_conf},job_id=12345678')

