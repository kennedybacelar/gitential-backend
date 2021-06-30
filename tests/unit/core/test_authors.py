import pytest
from gitential2.datatypes.authors import AuthorAlias, AuthorInDB
from gitential2.core.authors import alias_matching_author, authors_matching, tokenize_alias, aliases_matching


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


@pytest.mark.parametrize(
    "alias,author,expected",
    [
        (
            AuthorAlias(name="James Bond"),
            AuthorInDB(id=0, active=True, aliases=[AuthorAlias(name="John Doe")]),
            False,
        ),
        (
            AuthorAlias(name="John Doe"),
            AuthorInDB(id=0, active=True, aliases=[AuthorAlias(name="John Doe")]),
            True,
        ),
        (
            AuthorAlias(name="John Doe"),
            AuthorInDB(id=0, active=True, aliases=[AuthorAlias(email="john.doe@gitential.com")]),
            True,
        ),
        (
            AuthorAlias(login="john.doe"),
            AuthorInDB(id=0, active=True, aliases=[AuthorAlias(email="john.doe@gitential.com")]),
            True,
        ),
        (
            AuthorAlias(login="james007"),
            AuthorInDB(id=0, active=True, aliases=[AuthorAlias(email="john.doe@gitential.com")]),
            False,
        ),
    ],
)
def test_alias_matching_author(alias, author, expected):
    assert alias_matching_author(alias, author) == expected


@pytest.mark.parametrize(
    "first,second,expected",
    [
        (
            AuthorInDB(
                id=1,
                active=True,
                aliases=[AuthorAlias(login="James Bond")],
            ),
            AuthorInDB(
                id=2,
                active=True,
                aliases=[
                    AuthorAlias(
                        email="john.doe@gitential.com",
                    )
                ],
            ),
            False,
        ),
        (
            AuthorInDB(
                id=1,
                active=True,
                aliases=[AuthorAlias(login="John Doe"), AuthorAlias(email="another.email.for@testuser.com")],
            ),
            AuthorInDB(
                id=2,
                active=True,
                aliases=[
                    AuthorAlias(
                        email="john.doe@gitential.com",
                    )
                ],
            ),
            True,
        ),
    ],
)
def test_authors_matching(first, second, expected):
    assert authors_matching(first, second) == expected
