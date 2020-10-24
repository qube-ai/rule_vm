from .base import ActionConstant
from .send_email import SendEmailAction
from .relay import ChangeRelayState

ACTION_LUT = {
    ActionConstant.SEND_EMAIL.value: SendEmailAction,
    ActionConstant.CHANGE_RELAY_STATE.value: ChangeRelayState,
}
