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


class BaseInstruction:
    pass