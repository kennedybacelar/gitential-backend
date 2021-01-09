import os
from gitential2.utils.tempdir import TemporaryDirectory


def test_tempdir():
    with TemporaryDirectory() as temp_dir:
        assert temp_dir.name is not None
        filepath = temp_dir.new_file(content="Hello World!")
        assert open(filepath, "r").read() == "Hello World!"

    # The file has removed when the context ended.
    assert not os.path.exists(filepath)
