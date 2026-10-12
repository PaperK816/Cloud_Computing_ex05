import json
import os

class Storage:
    def __init__(self, node_id: int, data_dir: str = "data"):
        self.node_id = node_id
        self.data_dir = data_dir
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir, exist_ok=True)
        self.file_path = os.path.join(self.data_dir, f"node_{node_id}_state.json")
        self.load()

    def load(self):
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.promised_id = data.get("promised_id", None)
                    self.accepted_id = data.get("accepted_id", None)
                    self.accepted_value = data.get("accepted_value", None)
            except Exception as e:
                self._reset()
        else:
            self._reset()

    def _reset(self):
        self.promised_id = None
        self.accepted_id = None
        self.accepted_value = None

    def save(self):
        data = {
            "promised_id": self.promised_id,
            "accepted_id": self.accepted_id,
            "accepted_value": self.accepted_value
        }
        temp_path = self.file_path + ".tmp"
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(temp_path, self.file_path)

    def update_promise(self, promised_id):
        self.promised_id = promised_id
        self.save()

    def update_accepted(self, accepted_id, accepted_value):
        self.accepted_id = accepted_id
        self.accepted_value = accepted_value
        self.save()
