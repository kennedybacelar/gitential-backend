import json
from cryptography.fernet import Fernet


class FernetVault:
    def __init__(self, secret_key: bytes):
        self.f = Fernet(secret_key)
        self._vault = {}

    def __getitem__(self, key):
        return self._vault.__getitem__(key)

    def __setitem__(self, key, value):
        return self._vault.__setitem__(key, value)

    def __delitem__(self, key):
        return self._vault.__delitem__(key)

    def load(self, path):
        with open(path, "r") as f:
            contents = f.read()
            self._vault = {self.decrypt_string(k): self.decrypt_string(v) for k, v in json.loads(contents).items()}

    def save(self, path):
        with open(path, "w") as f:
            encrypted_vault = {self.encrypt_string(k): self.encrypt_string(v) for k, v in self._vault.items()}
            json_vault = json.dumps(encrypted_vault)
            f.write(json_vault)

    def encrypt_string(self, s: str) -> str:
        return self.f.encrypt(s.encode()).decode()

    def decrypt_string(self, s: str) -> str:
        return self.f.decrypt(s.encode()).decode()
