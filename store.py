from typing import Dict
from typing import List

import firebase_admin
from firebase_admin import firestore, credentials
from jsonschema import ValidationError, SchemaError
from loguru import logger
import trio

from vm import VM

FIREBASE_CREDENTIALS_FILE = "firebase_creds.json"

cred = credentials.Certificate(FIREBASE_CREDENTIALS_FILE)
firebase_app = firebase_admin.initialize_app(cred)
store = firestore.client()


def load_rules_from_db() -> List:
    rules_collection = store.collection("rules").get()
    list_of_rules = []

    # Iterate over all documents
    for document in rules_collection:
        d = document.to_dict()
        doc_id = document.id

        if "conditions" in d:
            rule_dict = d["conditions"]

            try:
                rule_obj = VM.parse_from_dict(rule_dict)
                rule_obj.set_id(doc_id)

                list_of_rules.append(rule_obj)
            except ValidationError:
                logger.error(f"ValidationError in parsing rule document -> {doc_id}")
            except SchemaError:
                logger.error(f"SchemaError in parsing rule document -> {doc_id}")
            except Exception as e:
                logger.error(f"Some unknown error occured. Error: {e}")
        else:
            logger.error(f"Missing `conditions` key in {doc_id}")

    return list_of_rules


async def get_document(collection: str, document: str):
    # Connect to firebase and get that information somehow here
    # Also check if we have the data cached somewhere, then return it from cache

    def f():
        return store.collection(collection).document(document).get()
    doc = await trio.to_thread.run_sync(f)

    if doc.exists:
        logger.debug(f"Fetched {doc} from Firestore.")
        return doc
    else:
        return False


async def update_document(collection: str, document: str, updated_dict: Dict):
    # Directly update documents in firebase and redis
    # TODO make it trio compatible async
    doc_ref = store.collection(collection).document(document)
    doc_ref.update(updated_dict)


async def get_device_document(device_id: str):
    # Get the document from Firebase somehow
    return await get_document("devices", device_id)


def get_device_document_sync(device_id: str):
    # This is a synchronous implementation of get_device_document
    doc = store.collection("devices").document(device_id).get()
    if doc.exists:
        logger.debug(f"Fetched {doc} from Firestore.")
        return doc.to_dict()
    else:
        return False


async def get_generated_data(device_id: str, count=5):

    def f():
        return store.collection("devices").document(device_id).collection("generatedData").limit(count).get()
    data = await trio.to_thread.run_sync(f)
    list_of_docs = []
    for x in data:
        list_of_docs.append(x.to_dict())

    logger.debug(f"Fetching generated data for {device_id}")
    return list_of_docs

