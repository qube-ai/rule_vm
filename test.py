from vm import VM
import time

rule_string = '''
AT_TIME_WITH_OCCURRENCE 00:30:00+05:30 9
'''
rule_obj = VM.parse_from_string(rule_string)
print(rule_obj)
print(rule_obj.instruction_stream)

test_vm = VM()
test_vm.execute_rule(rule_obj)

# Let the tasks load in the VM
time.sleep(4)

# Perform a waited join, wait for all tasks to finish
test_vm.waited_stop()
