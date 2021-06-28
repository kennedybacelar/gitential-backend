import pytest
from gitential2.datatypes.authors import AuthorAlias
from gitential2.core.authors import tokenize_alias, aliases_matching


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (AuthorAlias(name="Andrási László", email="laszlo.andrasi@gitential.com"), ["andrasi laszlo"]),
        (AuthorAlias(login="laszlo.andrasi"), ["andrasi laszlo"]),
        (AuthorAlias(email="mail@example.com"), []),
        (
            AuthorAlias(name="László, ANDRÁSI", email="laco@la.co.hu", login="andrasilaco"),
            ["andrasi laszlo", "andrasilaco"],
        ),
        (AuthorAlias(name="Александр Панченко"), ["aleksandr panchenko"]),
        (AuthorAlias(email="loverboy1984@gmail.com"), ["loverboy1984"]),
    ],
)
def test_tokenize_alias(test_input, expected):
    assert tokenize_alias(test_input) == expected


@pytest.mark.parametrize(
    "first,second,expected",
    [
        (AuthorAlias(name="László Andrási"), AuthorAlias(name="Andrási László"), True),
        (AuthorAlias(name="László Andrási"), AuthorAlias(email="laszlo.andrasi@gitential.com"), True),
        (AuthorAlias(name="First Test User"), AuthorAlias(name="Second Test User"), False),
        (AuthorAlias(name="Александр Панченко"), AuthorAlias(name="Aleksandr Panchenko"), True),
    ],
)
def test_aliases_matching(first, second, expected):
    assert aliases_matching(first, second) == expected
