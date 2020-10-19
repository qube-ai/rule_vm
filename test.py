from vm import VM

rule_string = '''
AT_TIME 06:30:00+05:30
AND  
AT_TIME_WITH_OCCURRENCE 18:00:00+05:30 54
AND
RELAY_STATE podnet-switch-1 0 on
'''
rule_obj = VM.parse_from_string(rule_string)
print(rule_obj)
print(rule_obj.instruction_stream)


# test_vm = VM()
# result = test_vm.execute_rule(rule_obj)

# print(result)


"""
Ideal way to parse strings:

<OPERATION> <TIME>

"""