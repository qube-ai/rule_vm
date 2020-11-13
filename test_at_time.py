import trio
from vm import VM

rule_string = """
AT_TIME_WITH_OCCURRENCE 09:37:00+05:30 0
"""

vm = VM()
rule = vm.parse_from_string(rule_string)
rule.set_periodic_execution(True)
vm.execute_rule(rule)
