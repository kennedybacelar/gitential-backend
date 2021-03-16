class GitentialException(Exception):
    pass


class NotImplementedException(GitentialException):
    pass


class NotFoundException(GitentialException):
    pass


class SettingsException(GitentialException):
    pass
