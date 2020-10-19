from .base import BaseInstruction
from .base import InstructionConstant


class DoorWindowState(BaseInstruction):
    instruction_type = InstructionConstant.DOOR_WINDOW_STATE
    name = "DW_STATE"


class DoorWindowStateFor(BaseInstruction):
    instruction_type = InstructionConstant.DOOR_WINDOW_STATE_FOR
    name = "DW_STATE_FOR"
