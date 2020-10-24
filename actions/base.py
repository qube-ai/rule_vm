from enum import Enum
from pathlib import Path
from typing import Dict
import os

import jsonschema
from dotenv import load_dotenv
from loguru import logger

# Load sendgrid credentials
env_path = Path(".") / "sendgrid.env"
load_dotenv(dotenv_path=env_path)


class ActionConstant(Enum):
    SEND_EMAIL = "SEND_EMAIL"
    CHANGE_RELAY_STATE = "CHANGE_RELAY_STATE"


class BaseAction:
    action_type = "BASE_ACTION"

    def __init__(self, action_data: Dict):
        self.validate(action_data)

    def validate(self, action_data):
        jsonschema.validate(
            action_data, self.schema, format_checker=jsonschema.draft7_format_checker
        )

    def __eq__(self, other):
        return self.action_type == other

    def __str__(self):
        return f"<Action '{self.action_type}'>"
