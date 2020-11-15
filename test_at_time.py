import trio
from vm import VM

rule_string = """
RELAY_STATE_FOR podnet-switch-1 1 1 265
"""

vm = VM()
rule = vm.parse_from_string(rule_string)
rule.set_periodic_execution(True)
vm.execute_rule(rule)
