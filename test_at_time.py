import trio
from vm import VM


rule_string = """
AT_TIME 09:01:00+05:30
"""

vm = VM()
rule = vm.parse_from_string(rule_string)
vm.execute_rule(rule)
