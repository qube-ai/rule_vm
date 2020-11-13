import trio
from vm import VM

rule_string = """
DW_STATE_FOR door-sensor-1 OPEN 15
"""

vm = VM()
rule = vm.parse_from_string(rule_string)
rule.set_periodic_execution(True)
vm.execute_rule(rule)
