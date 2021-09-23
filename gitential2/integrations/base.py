from abc import ABC, abstractmethod

from typing import Callable, List, Optional, Tuple
from datetime import datetime
from authlib.integrations.requests_client import OAuth2Session
from pydantic import BaseModel
from pydantic.datetime_parse import parse_datetime
from structlog import get_logger

from gitential2.settings import IntegrationSettings
from gitential2.datatypes.extraction import ExtractedKind
from gitential2.datatypes import UserInfoCreate, RepositoryInDB
from gitential2.datatypes.repositories import RepositoryCreate
from gitential2.datatypes.pull_requests import PullRequestData

from gitential2.extraction.output import OutputHandler


logger = get_logger(__name__)


class BaseIntegration:
    def __init__(self, name, settings: IntegrationSettings):
        self.name = name
        self.settings = settings
        self.integration_type = settings.type_

    @property
    def is_oauth(self) -> bool:
        return False


class OAuthLoginMixin(ABC):
    @property
    def is_oauth(self) -> bool:
        return True

    @abstractmethod
    def oauth_register(self) -> dict:
        pass

    def get_oauth2_client(self, **kwargs):
        params = self.oauth_register()
        params.update(kwargs)
        return OAuth2Session(**params)

    @abstractmethod
    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        pass

    @abstractmethod
    def refresh_token_if_expired(self, token, update_token: Callable) -> Tuple[bool, dict]:
        pass


class CollectPRsResult(BaseModel):
    prs_collected: List[int]
    prs_left: List[int]
    prs_failed: List[int]


class GitProviderMixin(ABC):
    @abstractmethod
    def get_client(self, token, update_token) -> OAuth2Session:
        pass

    def collect_pull_requests(
        self,
        repository: RepositoryInDB,
        token: dict,
        update_token: Callable,
        output: OutputHandler,
        author_callback: Callable,
        prs_we_already_have: Optional[dict] = None,
        limit: int = 200,
    ) -> CollectPRsResult:
        client = self.get_client(token=token, update_token=update_token)
        ret = CollectPRsResult(prs_collected=[], prs_left=[], prs_failed=[])

        if not self._check_rate_limit(token, update_token):
            return ret

        raw_prs = self._collect_raw_pull_requests(repository, client)
        logger.debug("Raw PRs collected", raw_prs=raw_prs)

        def _is_pr_up_to_date(pr: dict) -> bool:
            pr_number, updated_at = self._raw_pr_number_and_updated_at(pr)
            return (
                prs_we_already_have is not None
                and pr_number in prs_we_already_have
                and parse_datetime(prs_we_already_have[pr_number]) == updated_at
            )

        prs_needs_update = [pr for pr in raw_prs if not _is_pr_up_to_date(pr)]
        if not self._check_rate_limit(token, update_token):
            ret.prs_left = [self._raw_pr_number_and_updated_at(pr)[0] for pr in prs_needs_update]
            return ret

        counter = 0
        for pr in prs_needs_update:
            pr_number, _ = self._raw_pr_number_and_updated_at(pr)
            if counter >= limit:
                ret.prs_left.append(pr_number)
            else:
                pr_data = self.collect_pull_request(repository, token, update_token, output, author_callback, pr_number)
                if pr_data:
                    ret.prs_collected.append(pr_number)
                else:
                    ret.prs_failed.append(pr_number)
            counter += 1

        return ret

    def collect_pull_request(
        self,
        repository: RepositoryInDB,
        token: dict,
        update_token: Callable,
        output: OutputHandler,
        author_callback: Callable,
        pr_number: int,
    ) -> Optional[PullRequestData]:
        client = self.get_client(token=token, update_token=update_token)
        raw_data = None
        try:
            raw_data = self._collect_raw_pull_request(repository, pr_number, client)
            pr_data = self._tranform_to_pr_data(repository, pr_number, raw_data, author_callback)

            output.write(ExtractedKind.PULL_REQUEST, pr_data.pr)
            for commit in pr_data.commits:
                output.write(ExtractedKind.PULL_REQUEST_COMMIT, commit)
            for comment in pr_data.comments:
                output.write(ExtractedKind.PULL_REQUEST_COMMENT, comment)
            for label in pr_data.labels:
                output.write(ExtractedKind.PULL_REQUEST_LABEL, label)

            logger.debug("Updated/extracted pr", pr_number=pr_number, pr_data=pr_data)
            return pr_data
        except Exception:  # pylint: disable=broad-except
            logger.exception("Failed to extract PR", pr_number=pr_number, raw_data=raw_data)
            return None
        finally:
            client.close()

    # pylint: disable=unused-argument
    def _check_rate_limit(self, token, update_token):
        return True

    @abstractmethod
    def _collect_raw_pull_requests(self, repository: RepositoryInDB, client) -> list:
        pass

    @abstractmethod
    def _raw_pr_number_and_updated_at(self, raw_pr: dict) -> Tuple[int, datetime]:
        pass

    @abstractmethod
    def _collect_raw_pull_request(self, repository: RepositoryInDB, pr_number: int, client) -> dict:
        pass

    @abstractmethod
    def _tranform_to_pr_data(
        self, repository: RepositoryInDB, pr_number: int, raw_data: dict, author_callback: Callable
    ) -> PullRequestData:
        pass

    # @abstractmethod
    # def recalculate_pull_request(
    #     self,
    #     pr: PullRequest,
    #     repository: RepositoryInDB,
    #     token: dict,
    #     update_token: Callable,
    #     output: OutputHandler,
    # ) -> CollectPRsResult:
    #     pass

    @abstractmethod
    def list_available_private_repositories(
        self, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        pass

    @abstractmethod
    def search_public_repositories(
        self, query: str, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        pass
