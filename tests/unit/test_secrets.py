from cryptography.fernet import Fernet as CryptoFernet
from gitential2.secrets import FernetVault, Fernet
from gitential2.settings import GitentialSettings

TEST_FERNET_KEY = CryptoFernet.generate_key()


def test_vault_encrypt_string():
    vault = FernetVault(TEST_FERNET_KEY)
    sample_string = "árvíztűrő tükörfúrógép"
    encrypted_sample_string = vault.encrypt_string(sample_string)
    assert isinstance(encrypted_sample_string, str)
    assert encrypted_sample_string != sample_string
    decrypted_sample_string = vault.decrypt_string(encrypted_sample_string)
    assert decrypted_sample_string == sample_string


def test_vault_save_and_load(tmp_path):
    vault = FernetVault(TEST_FERNET_KEY)
    vault["foo"] = "bar"
    vault["baz"] = "bak"
    vault.save(tmp_path / "vault.json")
    vault_file_contents = open(tmp_path / "vault.json").read()
    for pattern in ["foo", "bar", "baz", "bak"]:
        assert pattern not in vault_file_contents

    new_vault = FernetVault(TEST_FERNET_KEY)
    new_vault.load(tmp_path / "vault.json")
    assert new_vault["foo"] == "bar"
    assert new_vault["baz"] == "bak"


def test_fernet():
    settings = GitentialSettings(secret="abcdefghabcdefghabcdefghabcdefgh", integrations={})
    f = Fernet(settings)
    sample_string = "árvíztűrő tükörfúrógép"
    encrypted_sample_string = f.encrypt_string(sample_string)
    assert encrypted_sample_string != sample_string
    decrypted_sample_string = f.decrypt_string(encrypted_sample_string)
    assert decrypted_sample_string == sample_string
