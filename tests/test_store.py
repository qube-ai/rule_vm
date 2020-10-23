import store
import trio


class TestStore:
    def test_get_device_document_sync(self):
        data = store.get_device_document_sync("test-podnet-switch-1")
        assert data["type"] == "switch"

    async def test_sleep(self):
        start_time = trio.current_time()
        await trio.sleep(1)
        end_time = trio.current_time()
        assert end_time - start_time >= 1