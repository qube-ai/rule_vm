import store


def test_get_device_document_sync():
    data = store.get_device_document_sync("test-podnet-switch-1")
    assert data["type"] == "switch"

