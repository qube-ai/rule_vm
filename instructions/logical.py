from .base import BaseInstruction
from .base import InstructionConstant
from typing import Dict


class LogicalAnd(BaseInstruction):
    instruction_type = InstructionConstant.LOGICAL_AND
    name = "LOGICAL_AND"

    def __init__(self, ins_data: Dict):
        # We don't use ins_data for this instruction
        pass

    def __eq__(self, other):
        return self.instruction_type == other


class LogicalOr(BaseInstruction):
    instruction_type = InstructionConstant.LOGICAL_OR
    name = "LOGICAL_OR"

    def __init__(self, ins_data: Dict):
        # We don't use ins_data for this instruction
        pass

    def __eq__(self, other):
        return self.instruction_type == other
