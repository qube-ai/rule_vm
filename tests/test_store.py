import store
import trio


class TestStore:
    def test_get_device_document_sync(self):
        data = store.get_device_document_sync("test-podnet-switch-1")
        assert data["type"] == "switch"

    async def test_load_rules_from_db(self):
        rules = store.load_rules_from_db()
        test_rule_1_doc = await store.get_document("rules", "test-rule-1")

        # Check if test-rule-1 exists
        flag = False
        for rule in rules:
            if rule.id == test_rule_1_doc.id:
                flag = True

        assert flag is True

    async def test_get_generated_data(self):
        data = await store.get_generated_data("podnet-switch-1", 5)
        assert len(data) == 5

    async def test_get_device_document(self):
        data = await store.get_device_document("test-podnet-switch-1")
        assert data.to_dict()["type"] == "switch"

    async def test_get_document(self):
        data = await store.get_document("devices", "test-podnet-switch-1")
        assert data.to_dict()["type"] == "switch"

    async def test_sleep(self):
        start_time = trio.current_time()
        await trio.sleep(1)
        end_time = trio.current_time()
        assert end_time - start_time >= 1
