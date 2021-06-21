from gitential2.utils.is_bugfix import calculate_is_bugfix

import pytest


test_data = [
    ([], "blablabla", False),
    ([], "blablaBugblabla", True),
    ([], "blablaFixblabla", True),
    (["bla", "alb"], "blablabla", False),
    (["bla", "alb", "Bug"], "blablabla", True),
    (["bla", "alb", "FiX"], "blablabla", True),
    (["bla", "alb", "Bug"], "BugFix", True),
]


@pytest.mark.parametrize("labels, title, result", test_data)
def test_calculate_is_bugfix(labels, title, result):
    assert calculate_is_bugfix(labels, title) is result
