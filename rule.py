import datetime

from loguru import logger

import store
from actions.lut import ACTION_LUT
from instructions import InstructionConstant
from instructions.lut import INSTRUCTION_LUT
import uuid


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
        last_execution=None,
        execution_count=0,
    ):
        # Generate and assign a unique ID for this rule
        # Same rules might have different UUID's
        self.rule_uuid = uuid.uuid4()
        self.id = id
        self.name = name
        self.description = description
        self.enabled = enabled
        self.conditions = conditions
        self.actions = actions
        self.last_execution = last_execution
        self.execution_count = execution_count
        # Devices that this rule uses for final evaluation
        self.dependent_devices = []

        self.instruction_stream = []
        self.action_stream = []

        self.parse_conditions()
        self.parse_actions()
        self.determine_device_dependencies()
        self.periodic_execution = True
        logger.debug(f"{self} dependent devices -> {self.dependent_devices}")

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

    def determine_device_dependencies(self):
        """Lists out all the devices on which the result of rule evaluation could change."""
        for ins in self.instruction_stream:
            if hasattr(ins, "device_id"):
                self.dependent_devices.append(ins.device_id)

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

    def set_last_execution(self, datetime):
        self.last_execution = datetime

    def set_execution_count(self, count):
        self.execution_count = count

    def set_periodic_execution(self, value):
        self.periodic_execution = value

    def update_rule_uuid(self):
        self.rule_uuid = uuid.uuid4()

    def create_clone(self):
        """Create a new copy of this object"""
        return Rule(
            id=self.id,
            name=self.name,
            description=self.description,
            enabled=self.enabled,
            conditions=self.conditions,
            actions=self.actions,
            last_execution=self.last_execution,
            execution_count=self.execution_count,
        )

    async def update_execution_info(self):
        self.execution_count += 1
        self.last_execution = datetime.datetime.now()
        await store.update_document(
            "rules",
            self.id,
            {
                "last_executed": self.last_execution,
                "execution_count": self.execution_count,
            },
        )
        logger.debug(
            f"Rule execution count({self.execution_count}) and last executed datetime ({self.last_execution}) updated."
        )

    def __str__(self):
        return f"<Rule({self.rule_uuid}): {self.id}>"

    def __eq__(self, other):
        return self.id == other

    def __repr__(self):
        return self.__str__()
