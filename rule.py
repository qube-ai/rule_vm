from typing import Dict
from typing import List

import instructions
from instructions import InstructionConstant


class RuleParsingException(Exception):
    pass


class InvalidInstructionException(RuleParsingException):
    pass


class Rule:
    """Rule is a collection of Conditions(Instructions) chained together."""

    # Look up table for instruction decoding
    instruction_lut: List = {
        "at_time": instructions.AtTime,
        "at_time_with_occurrence": instructions.AtTimeWithOccurrence,
        "relay_state": instructions.IsRelayState,
        "relay_state_for": instructions.IsRelayStateFor,
        "temperature": instructions.CheckTemperature,
        "temperature_for": instructions.CheckTemperatureFor,
        "door_window_state": instructions.DoorWindowState,
        "door_window_state_for": instructions.DoorWindowStateFor,
        "check_occupancy_for": instructions.CheckOccupancyFor,
        "check_occupancy": instructions.CheckOccupancy,
        "logical_and": instructions.LogicalAnd,
        "logical_or": instructions.LogicalOr,
        "energy_meter": instructions.EnergyMeter,
    }

    def __init__(self, rule_dict: Dict):
        """Converts list of dictionaries into rule object"""

        self.instruction_stream: List = []

        # Each rule has a bunch of instructions, Parse the instructions
        # Add add them to instruction_stream instance variable
        # Parse individual instructions
        for ins_data in rule_dict:
            if ins_data["operation"] in Rule.instruction_lut:
                Instruction = Rule.instruction_lut[ins_data["operation"]]
                self.instruction_stream.append(Instruction(ins_data))

            else:
                raise InvalidInstructionException(f"Unknown instruction: {ins_data['operation']}")

        self.infix_to_postfix()

    def infix_to_postfix(self):
        # Perform infix to postfix conversion to make it easier for the VM to evaluate the rule
        stack = []
        temp_ins = []

        for ins in self.instruction_stream:

            # If it's an operator, do this
            if (ins.instruction_type == InstructionConstant.LOGICAL_AND) or (ins.instruction_type == InstructionConstant.LOGICAL_OR):
                if len(stack) == 0:
                    stack.append(ins)
                else:
                    temp_ins.append(stack.pop())
                    stack.append(ins)

            # If it's an operand, simply throw it into temp_ins
            else:
                temp_ins.append(ins)

        # Put the remaining items from stack
        for ins in stack:
            temp_ins.append(ins)

        self.instruction_stream = temp_ins

    def __str__(self):
        return f"<Rule: {len(self.instruction_stream)} Instructions>"
