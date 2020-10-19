from vm import VM

rule_string = '''
AT_TIME 06:30:00+05:30
AND  
AT_TIME 18:00:00+05:30
'''
rule_obj = VM.parse_from_string(rule_string)
print(rule_obj)
# test_vm = VM()
# result = test_vm.execute_rule(rule_obj)

# print(result)


"""
Ideal way to parse strings:

<OPERATION> <TIME>

"""