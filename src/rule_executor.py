def pre_process():

    pass

def process(rule,context):
    failed_records = None
    return failed_records

def post_process(rule,context):
    summary = None
    return summary

def execute(rule , context):
    pre_process(rule, context)
    rule_summary = process(rule,context)
    failed_records = post_process(rule , context)
    return rule_summary , failed_records


