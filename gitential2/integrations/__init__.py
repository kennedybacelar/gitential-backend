from enum import Enum
from gitential2.settings import GitentialSettings
from .common import RepositorySource
from .gitlab import GitLabSource


class RepositorySourceType(str, Enum):
    gitlab = "gitlab"


def get_source_for(type_: RepositorySource):
    if type_ == RepositorySourceType.gitlab:
        return GitLabSource


def construct_login_configuration(settings: GitentialSettings, frontend_url):
    logins = {}
    for source_name, source_settings in settings.repository_sources.items():
        if source_settings.use_as_login:
            source_cls = get_source_for(source_settings.source_type)
            source = source_cls(source_name, settings)
            logins[source_name] = {
                "login_text": source_settings.login_text,
                "signup_text": source_settings.signup_text,
                "type": source_settings.source_type,
                "url": source.authentication_url(frontend_url),
            }
    return logins
