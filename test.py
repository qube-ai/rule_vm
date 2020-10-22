from vm import VM

rule_string = '''
AT_TIME 10:50:30+05:30
OR
AT_TIME 12:30:00+05:30
'''
rule_obj = VM.parse_from_string(rule_string)
print(rule_obj)
print(rule_obj.instruction_stream)

test_vm = VM()
test_vm.execute_rule(rule_obj)

# This does not allow the VM to execute the rules
# test_vm.stop()