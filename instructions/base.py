import json
from enum import Enum

import jsonschema


class InstructionConstant(Enum):
    # Operators
    LOGICAL_AND = 1
    LOGICAL_OR = 2

    # Operands
    AT_TIME = 3
    AT_TIME_WITH_OCCURENCE = 4
    IS_RELAY_STATE = 5
    IS_RELAY_STATE_FOR = 6
    CHECK_TEMPERATURE = 7
    CHECK_TEMPERATURE_FOR = 8
    DOOR_WINDOW_STATE = 9
    DOOR_WINDOW_STATE_FOR = 10
    CHECK_OCCUPANCY_FOR = 11
    CHECK_OCCUPANCY = 12
    ENERGY_METER = 13


class BaseInstruction:

    name = 'BASE_INSTRUCTION'
    rule = None

    def __init__(self, json_data, rule):
        self.json_data = json_data
        self.rule = rule
        self.validate_data()

    def validate_data(self):
        # This will raise ValidationError or SchemaError,
        # both of which we'll allow to propagate upwards
        jsonschema.validate(self.json_data, self.schema, format_checker=jsonschema.draft7_format_checker)

    def __str__(self):
        return f"<Instruction '{self.name}'>"

    def __repr__(self):
        return self.__str__()


class InstructionException(Exception):
    pass
