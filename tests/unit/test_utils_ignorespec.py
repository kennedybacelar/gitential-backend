from gitential2.utils.ignorespec import IgnoreSpec


def test_ignorespec_match_directory():
    spec = IgnoreSpec(["vendor", "docs/"])
    assert spec.should_ignore("vendor/some_file.py")
    assert spec.should_ignore("docs/readme.txt")
    assert not spec.should_ignore("gitential2.py")
    assert not spec.should_ignore("docs.py")


def test_ignorespec_match_wildcard():
    spec = IgnoreSpec(["*.java"])
    assert spec.should_ignore("/com/gitential/typical.java")
    assert not spec.should_ignore("/gitential2/utils/random.py")
