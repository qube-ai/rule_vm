import os

import trio
from loguru import logger
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To

from .base import BaseAction
from .base import ActionConstant
from typing import Dict


class SendEmailAction(BaseAction):
    action_type = ActionConstant.SEND_EMAIL

    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "type": {"type": "string", "enum": ["send_email"]},
            "subject": {"type": "string"},
            "body": {"type": "string"},
            "to": {"type": "array"},
        },
        "required": ["type", "subject", "body", "to"],
    }

    def __init__(self, action_data: Dict):
        super(SendEmailAction, self).__init__(action_data)
        self.subject = action_data["subject"]
        self.body = action_data["body"]
        self.to = action_data["to"]

    async def perform(self):
        from_email = Email("automated@thepodnet.com", name="Podnet")
        to_email = list(map(lambda x: To(x), self.to))
        message = Mail(
            from_email=from_email,
            to_emails=to_email,
            subject=self.subject,
            html_content=self.body,
        )

        def f():
            sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
            response = sg.send(message)
            logger.info(
                f"Email sent. Status Code: {response.status_code}, Body: {response.body}"
            )

        try:
            await trio.to_thread.run_sync(f)
        except Exception as e:
            logger.error(f"Unable to send the email due to some error. Error: {e}")
            logger.error(f"Error body: {e.body}")
