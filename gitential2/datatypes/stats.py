from typing import List, Optional, Dict, Any, Union
from enum import Enum
from pydantic import BaseModel


class MetricName(str, Enum):
    # Commit metrics
    count_commits = "count_commits"
    sum_loc_effort = "sum_loc_effort"
    sum_hours = "sum_hours"
    sum_ploc = "sum_ploc"
    efficiency = "efficiency"

    nunique_contributors = "nunique_contributors"

    # PR metrics
    avg_pr_commit_count = "avg_pr_commit_count"
    avg_pr_code_volume = "avg_pr_code_volume"
    avg_review_time = "avg_review_time"
    avg_pickup_time = "avg_pickup_time"
    avg_development_time = "avg_development_time"
    pr_merge_ratio = "pr_merge_ratio"
    sum_pr_closed = "sum_pr_closed"
    sum_pr_merged = "sum_pr_merged"
    sum_review_comment_count = "sum_review_comment_count"
    avg_pr_review_comment_count = "avg_pr_review_comment_count"
    sum_pr_count = "sum_pr_count"
    avg_pr_cycle_time = "avg_pr_cycle_time"


class FilterName(str, Enum):
    repo_ids = "repo_ids"
    emails = "emails"
    name = "name"
    day = "day"
    ismerge = "ismerge"
    keyword = "keyword"
    outlier = "outlier"
    commit_msg = "commit_msg"
    account_id = "account_id"
    project_id = "project_id"
    team_id = "team_id"


class DimensionName(str, Enum):
    day = "day"
    week = "week"
    month = "month"
    hour = "hour"
    repo_id = "repo_id"
    name = "name"
    istest = "istest"
    email = "email"
    newpath = "newpath"
    language = "language"
    commit_id = "commit_id"
    keyword = "keyword"


class StatsRequest(BaseModel):
    metrics: List[str]
    dimensions: Optional[List[str]] = None
    filters: Dict[str, Any]
    sort_by: Optional[List[Union[str, int]]] = None
    type: str = "aggregate"  # or "select"


class QueryType(str, Enum):
    aggregate = "aggregate"
    select = "select"


class TableName(str, Enum):
    commits = "commits"
    patches = "patches"
    pull_requests = "pull_requests"


class Query(BaseModel):
    metrics: List[MetricName]
    dimensions: Optional[List[DimensionName]] = None
    filters: Dict[FilterName, Any]
    sort_by: Optional[List[Union[str, int]]] = None
    type: QueryType


class QueryResult(BaseModel):
    query: Query
    results: Any


# class StatsRequest(BaseModel):
#     metrics: List[MetricName]
#     dimensions: Optional[List[DimensionName]] = None
#     filters: Dict[FilterName, Any]
#     sort_by: Optional[List[Union[str, int]]] = None
#     type  = "aggregate"  # or "select"
