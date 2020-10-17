import json
import store

json_rule = json.loads([
    {
        "type": "relay_state",
        "relay": 0
    },
    {
        "type":""
    }
])


class Rule:

    @staticmethod
    def from_json(data):
        '''Converts JSON data into rule object'''
        try:
            rule_dict = json.loads(data)
            
            r = Rule()
            # Create the rule object here.
            return r

        except json.JSONDecodeError:
            print("Unable to decode JSON")
            return None


class VM:

    def __init__(self):
        pass

    def add_rule(self, rule):
        pass


rule = Rule.from_json(json_rule)

vm = VM()
vm.add_rule(rule)