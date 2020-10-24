from loguru import logger

import store
from actions.lut import ACTION_LUT
from instructions import InstructionConstant
from instructions.lut import INSTRUCTION_LUT


class RuleParsingException(Exception):
    pass


class InvalidInstructionException(RuleParsingException):
    pass


class Rule:
    def __init__(
        self,
        id=None,
        name=None,
        description=None,
        enabled=True,
        conditions=[],
        actions=[],
    ):
        self.id = id
        self.name = name
        self.description = description
        self.enabled = enabled
        self.conditions = conditions
        self.actions = actions
        # Devices that this rule uses for final evaluation
        self.devices = []

        self.instruction_stream = []
        self.action_stream = []

        self.parse_conditions()
        self.parse_actions()

    def parse_conditions(self):
        for ins_data in self.conditions:
            operation = ins_data["operation"].upper()
            if operation in INSTRUCTION_LUT:
                Instruction = INSTRUCTION_LUT[operation]
                self.instruction_stream.append(Instruction(ins_data, self))

            else:
                logger.error(f"Incorrect/Unknown operation: {operation}")
                raise InvalidInstructionException(
                    f"Incorrect/Unknown operation: {operation}"
                )

        self.infix_to_postfix()

    def infix_to_postfix(self):
        """Perform infix to postfix conversion to make it easier for the VM to evaluate the rule"""
        stack = []
        temp_ins = []

        for ins in self.instruction_stream:

            # If it's an operator, do this
            if (ins.instruction_type == InstructionConstant.LOGICAL_AND) or (
                ins.instruction_type == InstructionConstant.LOGICAL_OR
            ):
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

    def parse_actions(self):
        i = 0
        for action_data in self.actions:
            if "type" in action_data:
                operation = action_data["type"].upper()
                if operation in ACTION_LUT:
                    Action = ACTION_LUT[operation]
                    self.action_stream.append(Action(action_data))

                else:
                    logger.error(
                        f"Incorrect/Unknown action type at index {i}: {operation}"
                    )

            else:
                logger.error(
                    f"{self.id}: Action @ index {i} has no `type` field attached."
                )

            i += 1

    async def get_rule_document(self):
        return await store.get_document("rules", self.id)

    def __str__(self):
        return f"<Rule({len(self.instruction_stream)}): {self.id}>"
