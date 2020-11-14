import trio
from vm import VM

rule_string = """
OCCUPANCY_FOR occupancy-sensor-1 OCCUPIED 2
"""

vm = VM()
rule = vm.parse_from_string(rule_string)
rule.set_periodic_execution(True)
vm.execute_rule(rule)
