import pytest

from gitential2.utils import calc_repo_namespace, levenshtein_ratio


@pytest.mark.parametrize(
    "clone_url,namespace",
    [
        ("https://github.com/Microsoft/vscode.git", "Microsoft"),
        ("https://gitlab.com/gitential-com/gitential2.git", "gitential-com"),
        ("https://brappleyeIII@bitbucket.org/cogitech/lashbrook-cart.git", "cogitech"),
        ("git@github.com:hewett-learning/hewett-learning-app.git", "hewett-learning"),
        ("ssh://git@bitbucket.org:cogitech/stampinup-hr.git", "cogitech"),
        ("ssh://git@bitbucket.org/cogitech/stampinup-hr.git", "cogitech"),
        ("https://laszloandrasi.visualstudio.com/first-project/_git/first-project", "first-project"),
        ("https://gitlab.com/gamesystems/devops/gitlab-ci-common.git", "gamesystems/devops"),
        ("git@gitlab.com:gamesystems/devops/gitlab-ci-common.git", "gamesystems/devops"),
    ],
)
def test_calc_repo_namespace(clone_url, namespace):
    assert calc_repo_namespace(clone_url) == namespace


@pytest.mark.parametrize(
    "s1,s2,ratio",
    [
        ("test", "test", 1),
        ("test", "best", 0.75),
        ("abcde", "abcdF", 0.8),
        ("abcde", "fghij", 0),
        ("long string", "another list of characters", 0.1923076),
    ],
)
def test_levenshtein_ratio(s1, s2, ratio):
    assert pytest.approx(levenshtein_ratio(s1, s2), ratio)
