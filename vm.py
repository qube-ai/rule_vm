import json
import queue
import sys
import threading
import time
import pickle
import os
import redis
import aioredis

import trio
from jsonschema import ValidationError, SchemaError
from loguru import logger
from parse import compile as pc

import instructions
import rule
import store


class VM:
    TASK_QUEUE_BUFFER_SIZE = 10
    FUTURE_TASK_QUEUE_BUFFER_SIZE = 10
    LIST_OF_RULES = []
    FUTURE_TASKS_AWAITING_COMPLETION = []
    TASKS_RUNNING = 0
    FUTURE_TASK_COUNT = 0
    # Used for parsing rules in string format
    instructions_pattern = [
        pc("AT_TIME {time}"),
        pc("AT_TIME_WITH_OCCURRENCE {time} {occurrence:d}"),
        pc("AND"),
        pc("OR"),
        pc("DW_STATE {device_id} {state}"),
        pc("DW_STATE_FOR {device_id} {state} {for:d}"),
        pc("OCCUPANCY_STATE {device_id} {state}"),
        pc("OCCUPANCY_FOR {device_id} {state} {for:d}"),
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

    def __init__(self, load_rules_from_disk=True):
        self.run_vm_thread = True
        self.load_rules_from_disk = load_rules_from_disk
        self.last_serialized_rules = []  # To prevent useless writing to disk
        self.task_queue = queue.Queue(self.TASK_QUEUE_BUFFER_SIZE)
        self.future_task_queue = queue.Queue(self.FUTURE_TASK_QUEUE_BUFFER_SIZE)

        if self.load_rules_from_disk:
            self.load_future_rules_from_disk_to_queue()

        self.vm_thread = threading.Thread(target=lambda: trio.run(self.__starter))
        self.vm_thread.start()
        logger.info("Started VM thread.")
        self.FUTURE_TASK_LIST_FILE_HANDLER = open(
            "future_task_list.pickle", "wb", buffering=0
        )

    def load_future_rules_from_disk_to_queue(self):
        if os.path.exists("future_task_list.pickle"):

            f = open("future_task_list.pickle", "rb")
            try:
                self.last_serialized_rules = pickle.load(f)
                for rule in self.last_serialized_rules:
                    logger.debug(f"Retrieved rule from disk ->  {rule}")
                    self.task_queue.put(rule)

            except EOFError:
                logger.error("EOFError. future_task_list.pickle file is empty.")

            f.close()
        else:
            logger.error("future_task_list.pickle file not found on disk.")

    async def __starter(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.task_spawner, nursery)
            logger.info("Started task spawner.")

            nursery.start_soon(self.future_task_serializer)
            logger.info("Started future task serializer.")

            nursery.start_soon(self.update_interface)
            logger.info("Started update interface.")

    async def update_interface(self):
        #     Open redis interface

        # redis = await aioredis.create_redis_pool(('localhost', 6379))
        r = redis.Redis(host='localhost', port=6379, db=0)
        while self.run_vm_thread:
            # Your code goes here

            tmp_rule_list = list(map(lambda x: str(x), self.LIST_OF_RULES))
            r.set("list_of_rules", json.dumps(tmp_rule_list))

            future_task_awaiting = list(map(lambda x: str(x), self.FUTURE_TASKS_AWAITING_COMPLETION))
            r.set("future_task_awaiting", json.dumps(future_task_awaiting))

            # for x in self.LIST_OF_RULES:
            #     r.lpush('list_of_rules', str(x))

            # for x in self.FUTURE_TASKS_AWAITING_COMPLETION:
            #     r.lpush('future_task_awaiting', str(x))
            r.set("running_tasks" , self.TASKS_RUNNING)
            r.set("future_tasks_count", self.FUTURE_TASK_COUNT)
            # do it every second
            await trio.sleep(1)



    async def future_task_serializer(self):
        while self.run_vm_thread:
            # Every 5 seconds serialize the contents of FUTURE_TASKS_AWAITING_COMPLETION list
            await trio.sleep(5)
            # logger.info("Starting FUTURE_TASKS serialization")

            def f():
                pickle.dump(
                    self.FUTURE_TASKS_AWAITING_COMPLETION,
                    self.FUTURE_TASK_LIST_FILE_HANDLER,
                )
                self.last_serialized_rules = self.FUTURE_TASKS_AWAITING_COMPLETION
                self.FUTURE_TASK_LIST_FILE_HANDLER.flush()

            if self.last_serialized_rules != self.FUTURE_TASKS_AWAITING_COMPLETION:
                logger.info(
                    f"{len(self.FUTURE_TASKS_AWAITING_COMPLETION)} rules are being serialized to disk."
                )
                await trio.to_thread.run_sync(f)

            else:
                pass
                # logger.info("No rules have changed. Nothing to serialize.")

    async def task_spawner(self, nursery):
        while self.run_vm_thread:

            # Look into active task_queue and run any rules if available
            if not self.task_queue.empty():
                rule_obj = self.task_queue.get_nowait()
                if rule_obj.enabled:
                    nursery.start_soon(self.__executor, nursery, rule_obj)
                    logger.info(f"Spawned a new task inside the VM: {rule_obj}")
                    self.TASKS_RUNNING += 1

                else:
                    logger.info(
                        f"{rule_obj} is currently disabled. Skipping execution."
                    )

            # Spawn new tasks that come into future_tasks_queue
            if not self.future_task_queue.empty():
                rule_obj, time_to_wait = self.future_task_queue.get_nowait()
                if rule_obj.enabled:
                    nursery.start_soon(self.__future_executor, rule_obj, time_to_wait)
                    logger.info(
                        f"{rule_obj} will be added as an active task in {time_to_wait} seconds"
                    )
                    self.FUTURE_TASK_COUNT += 1
                else:
                    logger.info(
                        f"Future task queue: {rule_obj} is currently disabled. Skipping execution."
                    )

            await trio.sleep(0)

    async def __future_executor(self, rule_obj, time_to_wait):
        """Wait for `time_to_wait` seconds and then execute the rule."""
        await trio.sleep(
            time_to_wait + 2
        )  # Add 2 seconds for definite execution next time
        self.execute_rule(rule_obj)
        logger.info(f"Added {rule_obj} back to active task queue")

        # Once the rule is scheduled for execution, remove it from FUTURE_TASKS list

        self.FUTURE_TASK_COUNT -= 1

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
                    op1_value = await op1.evaluate(self)
                else:
                    # It's probably only a bool value
                    op1_value = op1

                # Evaluate op2
                op2_value = None
                if isinstance(op2, instructions.BaseInstruction):
                    # If it's an instruction evaluate it
                    op2_value = await op2.evaluate(self)
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
            execute_action = await last_item.evaluate(self)
            logger.debug(f"Evaluation of {rule} returned {execute_action}")
            self.TASKS_RUNNING -= 1

        # It's just a boolean value, return it directly
        else:
            logger.debug(f"Evaluation of {rule} returned {last_item}")
            self.TASKS_RUNNING -= 1
            execute_action = last_item

        # Code to perform action
        if execute_action:
            # Update rule information
            if rule.id != "immediate":
                await rule.update_execution_info()

            logger.info(f"Executing {len(rule.action_stream)} action(s)")
            for action in rule.action_stream:
                logger.info(f"Spawned a new task to execute {action}")
                nursery.start_soon(action.perform)

        else:
            logger.info("Rule did not evaluate to True. No actions will be executed.")

        # One more thing...remove the task from FUTURE_TASKS_AWAITING_COMPLETION list
        # If it belongs to that list
        self.__remove_task_from_future_awaiting_completion(rule)

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
    def parse_from_string(rule_script: str) -> rule.Rule:
        rule_lines = rule_script.split("\n")

        parsed_json = []

        for line in rule_lines:
            clean_line = line.strip().lower()

            if clean_line.startswith(
                instructions.InstructionConstant.LOGICAL_AND.value.lower()
            ):
                parsed_json.append(
                    {"operation": instructions.InstructionConstant.LOGICAL_AND.value}
                )

            elif clean_line.startswith(
                instructions.InstructionConstant.LOGICAL_OR.value.lower()
            ):
                parsed_json.append(
                    {"operation": instructions.InstructionConstant.LOGICAL_OR.value}
                )

            else:
                for pattern in VM.instructions_pattern:
                    match = pattern.parse(clean_line)
                    if match:
                        # If match is found, move to the next line
                        operation = clean_line.split(" ")[0]
                        json_instruction = {**match.named}

                        # Check for each instruction
                        if (
                            operation
                            == instructions.InstructionConstant.AT_TIME.value.lower()
                        ):
                            json_instruction[
                                "operation"
                            ] = instructions.InstructionConstant.AT_TIME.value

                        elif (
                            operation
                            == instructions.InstructionConstant.AT_TIME_WITH_OCCURRENCE.value.lower()
                        ):
                            json_instruction[
                                "operation"
                            ] = (
                                instructions.InstructionConstant.AT_TIME_WITH_OCCURRENCE.value
                            )

                        elif (
                            operation
                            == instructions.InstructionConstant.ENERGY_METER.value.lower()
                        ):
                            json_instruction[
                                "operation"
                            ] = instructions.InstructionConstant.ENERGY_METER.value

                        elif (
                            operation
                            == instructions.InstructionConstant.DW_STATE.value.lower()
                        ):
                            json_instruction[
                                "operation"
                            ] = instructions.InstructionConstant.DW_STATE.value

                        elif (
                            operation
                            == instructions.InstructionConstant.DW_STATE_FOR.value.lower()
                        ):
                            json_instruction[
                                "operation"
                            ] = instructions.InstructionConstant.DW_STATE_FOR.value

                        elif (
                            operation
                            == instructions.InstructionConstant.OCCUPANCY.value.lower()
                        ):
                            json_instruction[
                                "operation"
                            ] = instructions.InstructionConstant.OCCUPANCY.value

                        elif (
                            operation
                            == instructions.InstructionConstant.OCCUPANCY_FOR.value.lower()
                        ):
                            json_instruction[
                                "operation"
                            ] = instructions.InstructionConstant.OCCUPANCY_FOR.value

                        elif (
                            operation
                            == instructions.InstructionConstant.RELAY_STATE.value.lower()
                        ):
                            json_instruction[
                                "operation"
                            ] = instructions.InstructionConstant.RELAY_STATE.value

                        elif (
                            operation
                            == instructions.InstructionConstant.RELAY_STATE_FOR.value.lower()
                        ):
                            json_instruction[
                                "operation"
                            ] = instructions.InstructionConstant.RELAY_STATE_FOR.value

                        elif (
                            operation
                            == instructions.InstructionConstant.TEMPERATURE.value.lower()
                        ):
                            json_instruction[
                                "operation"
                            ] = instructions.InstructionConstant.TEMPERATURE.value

                        elif (
                            operation
                            == instructions.InstructionConstant.TEMPERATURE_FOR.value.lower()
                        ):
                            json_instruction[
                                "operation"
                            ] = instructions.InstructionConstant.TEMPERATURE_FOR.value

                        else:
                            # This will probably never get executed
                            # The reason is, we'll never have a match with a string that
                            # we don't recognize.
                            print(f"Unknown operation: {operation}")

                        parsed_json.append(json_instruction)

                    else:
                        # If match isn't found, move to the next pattern
                        continue

        return VM.parse_from_dict(parsed_json)

    @staticmethod
    def parse_from_json(json_data: str) -> rule.Rule:
        try:
            parsed_json = json.loads(json_data)
            return VM.parse_from_dict(parsed_json)
        except json.JSONDecodeError:
            print("Unable to decode JSON")

    @staticmethod
    def parse_from_dict(rule_dict) -> rule.Rule:
        return rule.Rule(
            id="immediate",
            name="One shot Rule",
            description="This is a rule created using the VM APIs",
            conditions=rule_dict,
        )

    # Deprecated - Don't use this
    def load_rules_from_db(self):
        from rule import Rule

        rules = store.get_all_rules()
        list_of_rules = []
        for r in rules:
            doc_id = r.id
            document = r.to_dict()
            logger.debug(f"Parsing and constructing a rule obj for {doc_id}")
            try:
                rule_obj = Rule(
                    id=doc_id,
                    name=document["name"],
                    description=document["description"],
                    enabled=document["enabled"],
                    conditions=document["conditions"],
                    actions=document["actions"],
                )
                if rule_obj not in self.LIST_OF_RULES:
                    self.LIST_OF_RULES.append(rule_obj)

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
        for r in self.LIST_OF_RULES:
            self.execute_rule(r)

    def rule_in_future_task_list(self, rule: rule.Rule):
        for rule_obj in self.FUTURE_TASKS_AWAITING_COMPLETION:
            if rule == rule_obj:
                # We found an rule objects with same ID
                return True

        # We haven't found shit, return false
        return False

    def execute_all_dependent_rules(self, device_id):
        for r in self.LIST_OF_RULES:
            if device_id in r.dependent_devices:
                # Rule should not be scheduled for execution in FUTURE_TASKS
                if not self.rule_in_future_task_list(r):
                    self.execute_rule(r)
                    logger.info(
                        f"{r} scheduled for execution because new data arrived from {device_id}"
                    )

                else:
                    logger.info(
                        f"{r} already scheduled for execution in FUTURE_TASK_QUEUE"
                    )

    def document_to_rule_obj(self, document) -> rule.Rule:
        doc_id = document.id
        document = document.to_dict()
        logger.debug(f"Parsing and constructing a rule obj for {doc_id}")
        try:
            rule_obj = rule.Rule(
                id=doc_id,
                name=document["name"],
                description=document["description"],
                enabled=document["enabled"],
                conditions=document["conditions"],
                actions=document["actions"],
            )

            if "execution_count" in document:
                rule_obj.set_execution_count(document["execution_count"])

            return rule_obj

        except ValidationError as e:
            logger.error(f"ValidationError in parsing rule document {doc_id} -> {e}")

        except SchemaError as e:
            logger.error(f"SchemaError in parsing rule document {doc_id} -> {e}")

        except Exception as e:
            logger.error(f"Some unknown error occurred. Error: {e}")

    def add_rule(self, document):
        prev_rule_count = len(self.LIST_OF_RULES)
        rule_obj = self.document_to_rule_obj(document)
        if rule_obj is not None and rule_obj not in self.LIST_OF_RULES:
            logger.debug(f"Added {rule_obj} to LIST_OF_RULES")
            self.LIST_OF_RULES.append(rule_obj)

            # Just for the time being
            self.execute_rule(rule_obj)

        logger.debug(
            f"Rule count before addition: {prev_rule_count} and after {len(self.LIST_OF_RULES)}"
        )

    def update_rule(self, document):
        prev_rule_count = len(self.LIST_OF_RULES)
        rule_obj = self.document_to_rule_obj(document)

        if rule_obj is not None and rule_obj in self.LIST_OF_RULES:
            i = 0
            while i < len(self.LIST_OF_RULES):
                if rule_obj == self.LIST_OF_RULES[i]:
                    self.LIST_OF_RULES[i] = rule_obj
                    logger.debug(f"{rule_obj} was updated in LIST_OF_RULES")
                    break
                i += 1
        else:
            logger.debug(f"{rule_obj} was not found in LIST_OF_RULES. Adding it.")
            self.LIST_OF_RULES.append(rule_obj)
            logger.debug(
                f"{rule_obj} was added to the list. Since it was not present during the update."
            )

            # Just for the time being
            self.execute_rule(rule_obj)

        logger.debug(
            f"Rule count before addition: {prev_rule_count} and after {len(self.LIST_OF_RULES)}"
        )

    def remove_rule(self, document):
        prev_rule_count = len(self.LIST_OF_RULES)
        rule_obj = self.document_to_rule_obj(document)

        try:
            self.LIST_OF_RULES.remove(rule_obj)
            logger.debug(f"{rule_obj} was removed from LIST_OF_RULES")
        except ValueError:
            logger.debug(
                f"{rule_obj} was not found in the LIST_OF_RULES. So nothing to remove :D"
            )
        logger.debug(
            f"Rule count before addition: {prev_rule_count} and after {len(self.LIST_OF_RULES)}"
        )

    def rule_changed_callback(self, col_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == "ADDED":
                logger.info(f"Rule was ADDED - {change.document.id}")
                self.add_rule(change.document)
            elif change.type.name == "MODIFIED":
                logger.info(f"Rule was MODIFIED - {change.document.id}")
                self.update_rule(change.document)
            elif change.type.name == "REMOVED":
                logger.info(f"Rule was REMOVED - {change.document.id}")
                self.remove_rule(change.document)

    def sync_rules(self):
        firestore = store.store
        rules_col = firestore.collection("rules")
        rules_col.on_snapshot(self.rule_changed_callback)
        logger.info("Started the 'rules' collection watcher.")

    def add_rule_for_future_exec(self, rule_obj, time_to_execution):
        # Update rule_uuid to make sure the parent rule that added itself to FUTURE_TASKS_AWAITING_COMPLETION list
        # doesn't remove itself on finishing it's execution. So the parent's rule UUID and child's rule UUID
        # should be different. Hence we need to create a new rule_object clone.
        new_rule_obj = rule_obj.create_clone()
        self.FUTURE_TASKS_AWAITING_COMPLETION.append(new_rule_obj)
        self.future_task_queue.put((new_rule_obj, time_to_execution))

    def __remove_task_from_future_awaiting_completion(self, rule_obj):
        logger.debug(f"Looking for {rule_obj} in FUTURE_TASKS_AWAITING_COMPLETION")
        i = 0
        found = False
        for r in self.FUTURE_TASKS_AWAITING_COMPLETION:
            if r.rule_uuid == rule_obj.rule_uuid:
                logger.debug(
                    f"Rule found at index {i} in FUTURE_TASKS_AWAITING_COMPLETION"
                )
                found = True
                break
            i += 1

        if found:
            self.FUTURE_TASKS_AWAITING_COMPLETION.pop(i)
            logger.debug(f"Removed {rule_obj} from list of awaiting completion tasks.")
        else:
            logger.debug(
                f"{rule_obj} was not found in FUTURE_TASKS_AWAITING_COMPLETION."
            )
