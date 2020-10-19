from .base import BaseInstruction
from .base import InstructionConstant


class CheckOccupancy(BaseInstruction):
    instruction_type = InstructionConstant.CHECK_OCCUPANCY
    name = "CHECK_OCCUPANCY"


class CheckOccupancyFor(BaseInstruction):
    instruction_type = InstructionConstant.CHECK_OCCUPANCY_FOR
    name = "CHECK_OCCUPANCY_FOR"
