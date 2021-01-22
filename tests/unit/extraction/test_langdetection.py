from gitential2.extraction.langdetection import detect_lang, Langtype


def test_langdetection(test_repositories):
    repo = test_repositories["flask"]
    assert detect_lang(
        "src/flask/cli.py", 30906, False, "253570784cdcc85d82142128ce33e3b9d8f8db14", repo.directory
    ) == ("Python", Langtype.PROGRAMMING)

    assert detect_lang("somewhere/.bash_history") == ("Shell", Langtype.PROGRAMMING)
    assert detect_lang("example/index.js") == ("JavaScript", Langtype.PROGRAMMING)