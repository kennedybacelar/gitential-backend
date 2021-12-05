from gitential2.core.context import init_context_from_settings


def test_init_context_from_settings(settings_integration):
    g = init_context_from_settings(settings_integration)
    assert not list(g.backend.users.all())
    assert not list(g.backend.subscriptions.all())
    assert not list(g.backend.workspaces.all())
