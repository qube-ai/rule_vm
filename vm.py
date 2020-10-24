import queue
import threading
import instructions
import json
import time
import sys

import trio
import rule
from parse import compile as pc
from loguru import logger
import store
from jsonschema import ValidationError, SchemaError


class VM:
    TASK_QUEUE_BUFFER_SIZE = 10
    TASKS_RUNNING = 0
    # Used for parsing rules in string format
    instructions_pattern = [
        pc("AT_TIME {time}"),
        pc("AT_TIME_WITH_OCCURRENCE {time} {occurrence:d}"),
        pc("AND"),
        pc("OR"),
        pc("DW_STATE {device_id} {state}"),
        pc("DW_STATE_FOR {device_id} {state} {for:d}"),
        pc("OCCUPANCY_STATE {device_id} {state}"),
        pc("OCCUPANCY_STATE_FOR {device_id} {state} {for:d}"),
        pc("RELAY_STATE {device_id} {relay_index:d} {state}"),
        pc("RELAY_STATE_FOR {device_id} {relay_index:d} {state} {for:d}"),
        pc("TEMPERATURE {device_id} {comparison_op} {value:f}"),
        pc("TEMPERATURE_FOR {device_id} {comparison_op} {value:f} {for:d}"),
        pc("ENERGY_METER {device_id} VOLTAGE {comparison_op} {value:f}"),
        pc("ENERGY_METER {device_id} CURRENT {comparison_op} {value:f}"),
        pc("ENERGY_METER {device_id} REAL_POWER {comparison_op} {value:f}"),
        pc("ENERGY_METER {device_id} APPARENT_POWER {comparison_op} {value:f}"),
        pc("ENERGY_METER {device_id} POWER_FACTOR {comparison_op} {value:f}"),
        pc("ENERGY_METER {device_id} FREQUENCY {comparison_op} {value:f}"),
    ]

    def __init__(self):
        self.run_vm_thread = True
        self.task_queue = queue.Queue(self.TASK_QUEUE_BUFFER_SIZE)
        self.vm_thread = threading.Thread(target=lambda: trio.run(self.__starter))
        self.vm_thread.start()
        logger.info("Started VM thread.")

    async def __starter(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.task_spawner, nursery)
            # Started anything else required for the VM
            logger.info("Started task spawner.")

    async def task_spawner(self, nursery):
        while self.run_vm_thread:
            if not self.task_queue.empty():
                task = self.task_queue.get_nowait()
                nursery.start_soon(self.__executor, nursery, task)
                logger.info(f"Spawned a new task inside the VM: {task}")
                self.TASKS_RUNNING += 1

            await trio.sleep(0)

    async def __executor(self, nursery, rule):
        """Evaluates a rule using a stack."""
        logger.info(f"Executing: {rule}")
        # Stack used for evaluating a rule
        stack = []

        # Evaluate each expression
        for instruction in rule.instruction_stream:

            # If the instruction is a logical AND,
            # Pop operands from stack and perform AND operation
            # And put the result back into the stack
            if instructions.InstructionConstant.LOGICAL_AND == instruction:
                # Pop values from stack
                op1 = stack.pop()
                op2 = stack.pop()

                logger.debug(f"Evaluating AND instruction - {op1} AND {op2}")

                # Evaluate op1
                op1_value = None
                if isinstance(op1, instructions.BaseInstruction):
                    # If it's an instruction, evaluate it
                    op1_value = await op1.evaluate()
                else:
                    # It's probably only a bool value
                    op1_value = op1

                # Evaluate op2
                op2_value = None
                if isinstance(op2, instructions.BaseInstruction):
                    # If it's an instruction evaluate it
                    op2_value = await op2.evaluate()
                else:
                    # It's probably only a bool value
                    op2_value = op2

                # Perform logical AND and push it to stack
                stack.append(op1_value and op2_value)

            # If the instruction is a logical OR,
            # Pop operands from stack and perform OR operation
            # And put the result back into the stack
            elif instructions.InstructionConstant.LOGICAL_OR == instruction:
                # Pop values from stack
                op1 = stack.pop()
                op2 = stack.pop()

                logger.debug(f"Evaluating OR instruction - {op1} OR {op2}")

                # Evaluate op1
                op1_value = None
                if isinstance(op1, instructions.BaseInstruction):
                    # If it's an instruction, evaluate it
                    op1_value = await op1.evaluate()
                else:
                    # It's probably only a bool value
                    op1_value = op1

                # Evaluate op2
                op2_value = None
                if isinstance(op2, instructions.BaseInstruction):
                    # If it's an instruction evaluate it
                    op2_value = await op2.evaluate()
                else:
                    # It's probably only a bool value
                    op2_value = op2

                # Perform logical AND and push it to stack
                stack.append(op1_value or op2_value)

            # It's probably an operand, just put it in the stack
            else:
                logger.debug(f"Appending instruction {instruction} to stack.")
                stack.append(instruction)

        last_item = stack.pop()

        execute_action = False
        # If the last value is an Instruction, then the entire rule only had one instruction.
        # So evaluate the instruction and simply return it's value
        if isinstance(last_item, instructions.BaseInstruction):
            logger.info("Last item in stack is an unevaluated instruction.")
            execute_action = await last_item.evaluate()
            logger.debug(f"Evaluation of {rule} returned {execute_action}")
            self.TASKS_RUNNING -= 1

        # It's just a boolean value, return it directly
        else:
            logger.debug(f"Evaluation of {rule} returned {last_item}")
            self.TASKS_RUNNING -= 1
            execute_action = last_item

        # Code to perform action
        if execute_action:
            logger.info(f"Executing {len(rule.action_stream)} action(s)")
            for action in rule.action_stream:
                logger.info(f"Spawned a new task to execute {action}")
                nursery.start_soon(action.perform)

        else:
            logger.info("Rule did not evaluate to True. No actions will be executed.")

    def execute_rule(self, rule):
        # This function will not return anything, it would directly execute the rule
        self.task_queue.put(rule)

    def stop(self):
        logger.info("Shutting down VM thread. Awaiting join.")
        self.run_vm_thread = False
        self.vm_thread.join()

    def waited_stop(self):
        # Stops for all currently executing tasks to finish and then shuts down the VM
        while True:
            try:
                if self.TASKS_RUNNING == 0:
                    self.stop()
                    break
                else:
                    logger.info(f"Waiting for {self.TASKS_RUNNING} to finish.")
                time.sleep(1)
            except KeyboardInterrupt:
                logger.error("KeyboardInterrupt raised. Exiting.")
                sys.exit(0)

    @staticmethod
    def parse_from_string(rule_script: str) -> rule.Rule2:
        rule_lines = rule_script.split("\n")

        parsed_json = []

        for line in rule_lines:
            clean_line = line.strip().lower()

            if clean_line.startswith("and"):
                parsed_json.append({"operation": "logical_and"})

            elif clean_line.startswith("or"):
                parsed_json.append({"operation": "logical_or"})

            else:
                for pattern in VM.instructions_pattern:
                    match = pattern.parse(clean_line)
                    if match:
                        # If match is found, move to the next line
                        operation = clean_line.split(" ")[0]
                        json_instruction = {**match.named}

                        # Check for each instruction
                        if operation == "at_time":
                            json_instruction["operation"] = "at_time"

                        elif operation == "at_time_with_occurrence":
                            json_instruction["operation"] = "at_time_with_occurrence"

                        elif operation == "energy_meter":
                            json_instruction["operation"] = "energy_meter"

                        elif operation == "dw_state":
                            json_instruction["operation"] = "dw_state"

                        elif operation == "dw_state_for":
                            json_instruction["operation"] = "dw_state_for"

                        elif operation == "occupancy_state":
                            json_instruction["operation"] = "occupancy_state"

                        elif operation == "occupancy_state_for":
                            json_instruction["operation"] = "occupancy_state_for"

                        elif operation == "relay_state":
                            json_instruction["operation"] = "relay_state"

                        elif operation == "relay_state_for":
                            json_instruction["operation"] = "relay_state_for"

                        elif operation == "temperature":
                            json_instruction["operation"] = "temperature"

                        elif operation == "temperature_for":
                            json_instruction["operation"] = "temperature_for"

                        elif operation == "energy_meter":
                            json_instruction["operation"] = "energy_meter"

                        else:
                            # This will probably never get executed
                            # The reason is, we'll never have a match with a string that
                            # we don't recognize
                            print(f"Unknown operation: {operation}")

                        parsed_json.append(json_instruction)

                    else:
                        # If match isn't found, move to the next pattern
                        continue

        return VM.parse_from_dict(parsed_json)

    @staticmethod
    def parse_from_json(json_data: str) -> rule.Rule2:
        try:
            parsed_json = json.loads(json_data)
            return VM.parse_from_dict(parsed_json)
        except json.JSONDecodeError:
            print("Unable to decode JSON")

    @staticmethod
    def parse_from_dict(rule_dict) -> rule.Rule2:
        return rule.Rule2(
            id="immediate",
            name="One shot Rule",
            description="This is a rule created using the VM APIs",
            conditions=rule_dict,
        )

    def load_rules_from_db(self):
        from rule import Rule2

        rules = store.get_all_rules()
        list_of_rules = []
        for r in rules:
            doc_id = r.id
            document = r.to_dict()
            logger.debug(f"Parsing and constructing a rule obj for {doc_id}")
            try:
                rule_obj = Rule2(
                    id=doc_id,
                    name=document["name"],
                    description=document["description"],
                    enabled=document["enabled"],
                    conditions=document["conditions"],
                    actions=document["actions"],
                )
                list_of_rules.append(rule_obj)

            except ValidationError as e:
                logger.error(
                    f"ValidationError in parsing rule document {doc_id} -> {e}"
                )

            except SchemaError as e:
                logger.error(f"SchemaError in parsing rule document {doc_id} -> {e}")

            except Exception as e:
                logger.error(f"Some unknown error occurred. Error: {e}")

        logger.info(f"{len(list_of_rules)} rules were loaded in VM")

        # Execute the rules
        for x in list_of_rules:
            self.execute_rule(x)
