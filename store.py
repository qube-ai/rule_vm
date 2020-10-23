from typing import Dict

import firebase_admin
import trio
from firebase_admin import firestore, credentials
from loguru import logger

FIREBASE_CREDENTIALS_FILE = "firebase_creds.json"

cred = credentials.Certificate(FIREBASE_CREDENTIALS_FILE)
firebase_app = firebase_admin.initialize_app(cred)
store = firestore.client()


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


def get_all_rules():
    return store.collection("rules").get()


async def update_document(collection, document, data):
    def f():
        doc = store.collection(collection).document(document)
        doc.update(data)

    await trio.to_thread.run_sync(f)

