from pathlib import Path

from executor import execute

app_conf_file_path = Path(__file__).parent / "conf/properties/application.properties"
ruleset_conf_file_path = Path(__file__).parent / "conf/json/local/properties/employee_null_check.json"

rule_set_path =  str(ruleset_conf_file_path)
app_conf = str(app_conf_file_path)
execute(f'rule_set_path={rule_set_path},app_conf={rule_set_path},job_id=12345678')

