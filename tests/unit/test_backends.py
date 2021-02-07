from gitential2.backends.sql import SQLGitentialBackend
from gitential2.settings import GitentialSettings
from gitential2.datatypes import UserCreate, UserUpdate


def test_sql_backend():
    settings = GitentialSettings(
        backend="sql",
        backend_connection="sqlite:///:memory:",
        secret="test" * 8,
        integrations={},
    )
    backend = SQLGitentialBackend(settings)
    new_user = backend.users.create(UserCreate(login="abrakadabra", email="email@example.com"))
    user = backend.users.get(new_user.id)
    assert new_user == user

    print("new", new_user)
    print("get", user)
    updated_user = backend.users.update(new_user.id, UserUpdate(login="hokuszpokusz"))

    assert new_user.created_at == updated_user.created_at
    assert new_user.updated_at < updated_user.updated_at

    user = backend.users.get(new_user.id)

    assert updated_user == user

    deleted_user_count_first = backend.users.delete(new_user.id)
    assert deleted_user_count_first == 1
    deleted_user_count_second = backend.users.delete(new_user.id)
    assert deleted_user_count_second == 0

    assert backend.users.get(new_user.id) is None
