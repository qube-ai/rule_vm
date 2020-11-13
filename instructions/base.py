from enum import Enum

import jsonschema


class InstructionConstant(Enum):
    # Operators
    LOGICAL_AND = "LOGICAL_AND"
    LOGICAL_OR = "LOGICAL_OR"

    # Operands
    AT_TIME = "AT_TIME"
    AT_TIME_WITH_OCCURRENCE = "AT_TIME_WITH_OCCURRENCE"
    RELAY_STATE = "RELAY_STATE"
    RELAY_STATE_FOR = "RELAY_STATE_FOR"
    TEMPERATURE = "TEMPERATURE"
    TEMPERATURE_FOR = "TEMPERATURE_FOR"
    DW_STATE = "DW_STATE"
    DW_STATE_FOR = "DW_STATE_FOR"
    OCCUPANCY = "OCCUPANCY"
    OCCUPANCY_FOR = "OCCUPANCY_FOR"
    ENERGY_METER = "ENERGY_METER"


class BaseInstruction:

    name = "BASE_INSTRUCTION"
    rule = None

    def __init__(self, json_data, rule):
        self.json_data = json_data
        self.rule = rule
        self.validate_data()

    def evaluate(self, vm_instance):
        pass

    def validate_data(self):
        # This will raise ValidationError or SchemaError,
        # both of which we'll allow to propagate upwards
        jsonschema.validate(
            self.json_data, self.schema, format_checker=jsonschema.draft7_format_checker
        )

    def __str__(self):
        return f"<Instruction '{self.name}'>"

    def __repr__(self):
        return self.__str__()


class InstructionException(Exception):
    pass
