# pylint: disable=too-complex,too-many-branches
from typing import Generator, List, Any, Dict, Optional
from datetime import datetime, date, timedelta, timezone

from structlog import get_logger
from pydantic import BaseModel
import pandas as pd
import numpy as np
import ibis


from gitential2.datatypes.stats import (
    IbisTables,
    PR_METRICS,
    Query,
    MetricName,
    DimensionName,
    FilterName,
    QueryType,
    RELATIVE_DATE_DIMENSIONS,
    TableDef,
    TableName,
    DATE_DIMENSIONS,
)
from gitential2.datatypes.pull_requests import PullRequestState

from .context import GitentialContext


logger = get_logger(__name__)


class QueryResult(BaseModel):
    query: Query
    values: pd.DataFrame

    class Config:
        arbitrary_types_allowed = True


def _prepare_dimensions(dimensions, table_def: TableDef, ibis_tables, ibis_table):
    ret = []
    # Try this out first
    # if (DimensionName.name in dimensions or DimensionName.email in dimensions) and DimensionName.aid not in dimensions:
    #     dimensions.append(DimensionName.aid)
    for dimension in dimensions:
        res = _prepare_dimension(dimension, table_def, ibis_tables, ibis_table)
        if res is not None:
            ret.append(res)
    return ret


# pylint: disable=too-many-return-statements
def _prepare_dimension(
    dimension: DimensionName, table_def: TableDef, ibis_tables: IbisTables, ibis_table
):  # pylint: disable=too-complex
    if dimension in DATE_DIMENSIONS:
        if TableName.pull_requests in table_def:
            date_field_name = "created_at"
            # ibis_table = ibis_tables.pull_requests
        else:
            date_field_name = "date"
            # ibis_table = ibis_tables.commits

        if dimension == DimensionName.day:
            return (ibis_table[date_field_name].date().epoch_seconds() * 1000).name("date")
        elif dimension == DimensionName.week:
            return (ibis_table[date_field_name].date().truncate("W").epoch_seconds() * 1000).name("date")
        elif dimension == DimensionName.month:
            return (ibis_table[date_field_name].date().truncate("M").epoch_seconds() * 1000).name("date")
        elif dimension == DimensionName.hour:
            return (ibis_table[date_field_name].truncate("H").epoch_seconds() * 1000).name("date")

    elif dimension in RELATIVE_DATE_DIMENSIONS:
        if TableName.pull_requests in table_def:
            date_field_name = "created_at"
        else:
            date_field_name = "date"

        if dimension == DimensionName.day_of_week:
            return (ibis_table[date_field_name].date().day_of_week.index()).name("day_of_week")
        elif dimension == DimensionName.hour_of_day:
            return (ibis_table[date_field_name].hour()).name("hour_of_day")

    elif dimension == DimensionName.pr_state:
        return ibis_tables.pull_requests.state.name("pr_state")

    elif dimension == DimensionName.language:
        return ibis_table["lang"].name("language")
    elif dimension == DimensionName.name:
        return ibis_table["name"].name("name")
    elif dimension == DimensionName.email:
        return ibis_table["email"].name("email")
    elif dimension == DimensionName.repo_id:
        return ibis_table["repo_id"].name("repo_id")
    elif dimension == DimensionName.aid and TableName.pull_requests not in table_def:
        return ibis_table["aid"].name("aid")
    elif dimension == DimensionName.developer_id and TableName.pull_requests not in table_def:
        return ibis_table["aid"].name("developer_id")
    elif dimension == DimensionName.developer_id and TableName.pull_requests in table_def:
        return ibis_table["user_aid"].name("developer_id")
    return None


def _prepare_metrics(metrics, table_def: TableDef, ibis_tables, ibis_table, q: Query):
    ret = []
    for metric in metrics:
        if TableName.commits in table_def:
            res = _prepare_commits_metric(metric, ibis_table, q)
        elif TableName.pull_requests in table_def:
            res = _prepare_prs_metric(metric, ibis_tables)
        elif TableName.patches in table_def:
            res = _prepare_patch_metric(metric, ibis_table)
        if res is not None:
            ret.append(res)
    return ret


def _prepare_commits_metric(metric: MetricName, ibis_table, q: Query):
    # t = ibis_tables
    commits = ibis_table

    # commit metrics
    count_commits = commits.count().name("count_commits")

    loc_effort = commits.loc_effort_c.sum().name("sum_loc_effort")
    avg_loc_effort = commits.loc_effort_c.mean().name("avg_loc_effort")

    sum_hours = commits.hours.sum().name("sum_hours")
    avg_hours = commits.hours.mean().name("avg_hours")
    sum_ploc = (commits.loc_i_c.sum() - commits.uploc_c.sum()).name("sum_ploc")
    sum_uploc = commits.uploc_c.sum().name("sum_uploc")
    efficiency = (sum_ploc / (commits.loc_i_c.sum()).nullif(0) * 100).name("efficiency")
    nunique_contributors = commits.aid.nunique().name("nunique_contributors")
    comp_sum = (commits.comp_i_c.sum() - commits.comp_d_c.sum()).name("comp_sum")
    utilization = (sum_hours / q.utilization_working_hours() * 100).name("utilization")
    avg_velocity = commits.velocity.mean().name("avg_velocity")
    loc_sum = commits.loc_i_c.sum().name("loc_sum")

    commit_metrics = {
        MetricName.count_commits: count_commits,
        MetricName.sum_loc_effort: loc_effort,
        MetricName.avg_loc_effort: avg_loc_effort,
        MetricName.sum_hours: sum_hours,
        MetricName.avg_hours: avg_hours,
        MetricName.sum_ploc: sum_ploc,
        MetricName.sum_uploc: sum_uploc,
        MetricName.efficiency: efficiency,
        MetricName.nunique_contributors: nunique_contributors,
        MetricName.comp_sum: comp_sum,
        MetricName.utilization: utilization,
        MetricName.avg_velocity: avg_velocity,
        MetricName.loc_sum: loc_sum,
    }
    if metric not in commit_metrics:
        raise ValueError(f"missing metric {metric}")
    return commit_metrics.get(metric)


def _prepare_patch_metric(metric: MetricName, ibis_table):
    # pylint: disable=singleton-comparison, compare-to-zero
    if metric == MetricName.sum_loc_test:
        return ibis_table.loc_i.sum(where=ibis_table.is_test == True).name("sum_loc_test")
    elif metric == MetricName.sum_loc_impl:
        return ibis_table.loc_i.sum(where=ibis_table.is_test == False).name("sum_loc_impl")
    elif metric == MetricName.loc_effort_p:
        return ibis_table.loc_effort_p.sum().name("loc_effort_p")
    else:
        return None


def _prepare_prs_metric(metric: MetricName, ibis_tables: IbisTables):

    prs = ibis_tables.pull_requests

    sum_pr_count = prs.count().name("sum_pr_count")
    sum_pr_open = prs.title.count(where=prs.state == PullRequestState.open).name("sum_pr_open")
    sum_pr_closed = prs.title.count(where=prs.state == PullRequestState.closed).name("sum_pr_closed")
    sum_pr_merged = prs.title.count(where=prs.state == PullRequestState.merged).name("sum_pr_merged")
    avg_pr_commit_count = prs["commits"].mean().name("avg_pr_commit_count")
    avg_pr_code_volume = prs["additions"].mean().name("avg_pr_code_volume")
    avg_pr_cycle_time = (
        ((prs["merged_at"].epoch_seconds() - prs["first_commit_authored_at"].epoch_seconds()).abs() / 3600)
        .mean()
        .name("avg_pr_cycle_time")
    )
    avg_review_time = (
        ((prs["merged_at"].epoch_seconds() - prs["first_reaction_at"].epoch_seconds()).abs() / 3600)
        .mean()
        .name("avg_review_time")
    )
    avg_pickup_time = (
        ((prs["first_reaction_at"].epoch_seconds() - prs["created_at"].epoch_seconds()).abs() / 3600)
        .mean()
        .name("avg_pickup_time")
    )

    avg_development_time = (
        ((prs["created_at"].epoch_seconds() - prs["first_commit_authored_at"].epoch_seconds()).abs() / 3600)
        .mean()
        .name("avg_development_time")
    )
    sum_review_comment_count = prs["commits"].sum().name("sum_review_comment_count")
    avg_pr_review_comment_count = prs["commits"].mean().name("avg_pr_review_comment_count")

    pr_merge_ratio = (sum_pr_merged / sum_pr_count.nullif(0) * 100).name("pr_merge_ratio")

    pr_metrics = {
        MetricName.sum_pr_count: sum_pr_count,
        MetricName.avg_pr_commit_count: avg_pr_commit_count,
        MetricName.avg_pr_code_volume: avg_pr_code_volume,
        MetricName.avg_pr_cycle_time: avg_pr_cycle_time,
        MetricName.avg_review_time: avg_review_time,
        MetricName.avg_pickup_time: avg_pickup_time,
        MetricName.avg_development_time: avg_development_time,
        MetricName.sum_review_comment_count: sum_review_comment_count,
        MetricName.avg_pr_review_comment_count: avg_pr_review_comment_count,
        MetricName.sum_pr_open: sum_pr_open,
        MetricName.sum_pr_closed: sum_pr_closed,
        MetricName.sum_pr_merged: sum_pr_merged,
        MetricName.pr_merge_ratio: pr_merge_ratio,
    }

    return pr_metrics.get(metric)


def _get_author_ids_from_emails(g: GitentialContext, workspace_id: int, emails: List[str]):
    ret = []
    emails_set = set(emails)
    for author in g.backend.authors.all(workspace_id):
        if author.all_emails.intersection(emails_set):
            ret.append(author.id)
    return ret


def _prepare_filters_dict(
    g: GitentialContext,
    workspace_id: int,
    filters: Dict[FilterName, Any],
):
    filters_dict: dict = {}

    for filter_name, filter_params in filters.items():
        if filter_name == FilterName.emails:
            author_ids = _get_author_ids_from_emails(g, workspace_id, filter_params)
            filters_dict[FilterName.developer_ids] = author_ids
        if filter_name == FilterName.account_id:
            continue
        elif filter_name == FilterName.project_id:
            if filter_params:
                repo_ids = g.backend.project_repositories.get_repo_ids_for_project(
                    workspace_id=workspace_id, project_id=filter_params
                )
            else:
                repo_ids = []
            filters_dict[FilterName.repo_ids] = repo_ids
        elif filter_name == FilterName.repo_ids:
            filters_dict[FilterName.repo_ids] = filter_params
        elif filter_name == FilterName.author_ids:
            filters_dict[FilterName.developer_ids] = filter_params
        elif filter_name == FilterName.team_id:
            if filter_params:
                author_ids = g.backend.team_members.get_team_member_author_ids(
                    workspace_id=workspace_id, team_id=filter_params
                )
            else:
                author_ids = []
            filters_dict[FilterName.developer_ids] = author_ids
        elif filter_name == FilterName.day:
            filters_dict[FilterName.day] = filter_params
        elif filter_name == FilterName.is_bugfix:
            filters_dict[FilterName.is_bugfix] = filter_params
        elif filter_name == FilterName.is_merge:
            filters_dict[FilterName.is_merge] = filter_params
        elif filter_name == FilterName.ismerge:
            filters_dict[FilterName.is_merge] = filter_params
        elif filter_name == FilterName.is_new_code:
            filters_dict[FilterName.is_new_code] = filter_params
        elif filter_name == FilterName.is_collaboration:
            filters_dict[FilterName.is_collaboration] = filter_params
        elif filter_name == FilterName.is_pr_open:
            filters_dict[FilterName.is_pr_open] = filter_params
        elif filter_name == FilterName.is_pr_closed:
            filters_dict[FilterName.is_pr_closed] = filter_params
        elif filter_name == FilterName.is_pr_exists:
            filters_dict[FilterName.is_pr_exists] = filter_params
        elif filter_name == FilterName.active:
            filters_dict[FilterName.active] = filter_params
        else:
            logger.warning("Unhandled filter name", filter_name=filter_name, filter_params=filter_params)

    return filters_dict


def _prepare_filters(  # pylint: disable=too-complex
    g: GitentialContext,
    workspace_id: int,
    filters: Dict[FilterName, Any],
    table_def: TableDef,
    ibis_table,
) -> list:
    filters_dict = _prepare_filters_dict(g, workspace_id, filters)

    _ibis_filters: dict = {
        TableName.commits: {
            FilterName.repo_ids: lambda t: t.repo_id.isin,
            FilterName.author_ids: lambda t: t.aid.isin,
            FilterName.developer_ids: lambda t: t.aid.isin,
            FilterName.emails: lambda t: t.aemail.isin,
            "aids": lambda t: t.aid.isin,
            "name": lambda t: t.aname.isin,
            FilterName.day: lambda t: t.date.between,
            FilterName.is_merge: lambda t: t.is_merge.__eq__,
            FilterName.ismerge: lambda t: t.is_merge.__eq__,
            FilterName.is_bugfix: lambda t: t.is_bugfix.__eq__,
            FilterName.is_pr_open: lambda t: t.is_pr_open.__eq__,
            FilterName.is_pr_closed: lambda t: t.is_pr_closed.__eq__,
            FilterName.is_pr_exists: lambda t: t.is_pr_exists.__eq__,
            # "keyword": t.message.lower().re_search,
            # "outlier": t.outlier.__eq__,
            # "commit_msg": t.message.lower().re_search,
        },
        TableName.pull_requests: {
            FilterName.repo_ids: lambda t: t.repo_id.isin,
            FilterName.day: lambda t: t.created_at.between,
            FilterName.is_bugfix: lambda t: t.is_bugfix.__eq__,
            FilterName.developer_ids: lambda t: t.user_aid.isin,
        },
        TableName.patches: {
            FilterName.repo_ids: lambda t: t.repo_id.isin,
            FilterName.author_ids: lambda t: t.aid.isin,
            FilterName.developer_ids: lambda t: t.aid.isin,
            FilterName.emails: lambda t: t.aemail.isin,
            FilterName.day: lambda t: t.date.between,
            FilterName.is_merge: lambda t: t.is_merge.__eq__,
            FilterName.ismerge: lambda t: t.ismerge.__eq__,
            FilterName.is_collaboration: lambda t: t.is_collaboration.__eq__,
            FilterName.is_new_code: lambda t: t.is_new_code.__eq__,
        },
    }

    ret = []
    for filter_key, values in filters_dict.items():
        if filter_key in _ibis_filters.get(table_def[0], {}):
            filter_ = _ibis_filters[table_def[0]][filter_key](ibis_table)
            if filter_.__name__ == "isin":
                ret.append(filter_(values))
            elif isinstance(values, list):
                ret.append(filter_(*values))
            else:
                ret.append(filter_(values))
        else:
            logger.warning("FILTERKEY_MISSING", filter_key=filter_key, table_def=table_def)

    return ret


def _prepare_sort_by(query: Query):
    if (
        not set({DimensionName.day, DimensionName.week, DimensionName.month, DimensionName}).isdisjoint(
            set(query.dimensions or [])
        )
        and not query.sort_by
    ):
        logger.debug("adding date sort_by", dimensions=query.dimensions, sort_by=query.sort_by)
        return ["date"]
    else:
        return query.sort_by


class IbisQuery:
    def __init__(self, g: GitentialContext, workspace_id: int, query: Query):
        self.g = g
        self.workspace_id = workspace_id
        self.query = query

    def execute(self) -> QueryResult:
        logger.debug("Executing query", query=self.query, workspace_id=self.workspace_id)
        ibis_tables = self.g.backend.get_ibis_tables(self.workspace_id)
        ibis_table = ibis_tables.get_table(self.query.table)
        ibis_metrics = _prepare_metrics(self.query.metrics, self.query.table, ibis_tables, ibis_table, self.query)
        ibis_dimensions = (
            _prepare_dimensions(self.query.dimensions, self.query.table, ibis_tables, ibis_table)
            if self.query.dimensions
            else None
        )
        # ibis_dimensions = None
        ibis_filters = _prepare_filters(self.g, self.workspace_id, self.query.filters, self.query.table, ibis_table)

        if ibis_metrics:
            if self.query.type == QueryType.aggregate:
                # ibis_table.aggregate(ibis_metrics, by=query.dimensions).filter(query.filters)
                ibis_query = ibis_table.aggregate(metrics=ibis_metrics, by=ibis_dimensions).filter(ibis_filters)
            else:
                ibis_query = ibis_table.filter(ibis_filters).select(ibis_metrics)

            compiled = ibis.postgres.compile(ibis_query)
            logger.debug("**IBIS QUERY**", compiled_query=str(compiled), query=ibis_query)

            result = ibis_tables.conn.execute(ibis_query)
        else:
            result = pd.DataFrame()

        result = _sort_dataframe(result, query=self.query)
        return QueryResult(query=self.query, values=result)


def _sort_dataframe(result: pd.DataFrame, query: Query) -> pd.DataFrame:
    sort_by = _prepare_sort_by(query)
    if sort_by and not result.empty:
        logger.debug("SORTING", columns=result.columns, sort_by=sort_by)
        if isinstance(sort_by[0], list):
            by, ascending = map(list, zip(*sort_by))
            result.sort_values(by=by, ascending=ascending, inplace=True)
        else:
            result.sort_values(by=sort_by, inplace=True)

    logger.debug("RESULT AFTER SORTING", result=result)
    return result


def _to_jsonable_result(result: QueryResult) -> dict:
    if result.values.empty:
        return {}
    ret = result.values.replace([np.inf, -np.inf], np.nan)
    ret = ret.where(pd.notnull(ret), None)
    logger.debug("INDEX", index=ret.index)
    return ret.to_dict(orient="list")


def _add_missing_timestamp_to_result(result: QueryResult):
    if result.values.empty:
        return result
    date_dimension = _get_date_dimension(result.query)
    day_filter = result.query.filters.get(FilterName.day)
    if not date_dimension or not day_filter:
        return result

    from_date = datetime.strptime(day_filter[0], "%Y-%m-%d").date()
    to_date = datetime.strptime(day_filter[1], "%Y-%m-%d").date()

    all_timestamps = [
        int(ts.timestamp()) * 1000 for ts in _calculate_timestamps_between(date_dimension, from_date, to_date)
    ]
    date_col = ""
    if "datetime" in result.values.columns:
        date_col = "datetime"
    elif "date" in result.values.columns:
        date_col = "date"
    # print(result.values.columns)
    for ts in all_timestamps:
        if True not in (result.values[date_col] == ts).values:
            if True in (result.values[date_col] > ts).values:
                row = _create_empty_row(ts, date_col, result.values.columns, 0)
            else:
                row = _create_empty_row(ts, date_col, result.values.columns, None)
            result.values = result.values.append(row, ignore_index=True)
    result.values = _sort_dataframe(result.values, query=result.query)
    return result


def _create_empty_row(ts: int, date_column: str, column_list, default_field_value: Optional[int]) -> dict:
    ret: dict = {}
    default_values: dict = {
        "language": "Others",
        "name": "",
        "email": "",
    }
    for col in column_list:
        if col == date_column:
            ret[col] = ts
        elif col in default_values:
            ret[col] = default_values[col]
        else:
            ret[col] = default_field_value
    return ret


def _get_date_dimension(query: Query) -> Optional[DimensionName]:
    if query.dimensions:
        for d in query.dimensions:
            if d in DATE_DIMENSIONS:
                return d
    return None


def _start_of_the_day(day: date) -> datetime:
    return datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)


def _end_of_the_day(day: date) -> datetime:
    return datetime.combine(day, datetime.max.time(), tzinfo=timezone.utc)


def _next_hour(d: datetime) -> datetime:
    return d + timedelta(hours=1)


def _next_day(d: datetime) -> datetime:
    return d + timedelta(days=1)


def _next_week(d: datetime) -> datetime:
    return d + timedelta(days=7)


def _next_month(d: datetime) -> datetime:
    if d.month == 12:
        return d.replace(year=d.year + 1, month=1)
    else:
        return d.replace(month=d.month + 1)


def _calculate_timestamps_between(
    date_dimension: DimensionName, from_date: date, to_date: date
) -> Generator[datetime, None, None]:
    from_ = _start_of_the_day(from_date)
    to_ = _end_of_the_day(to_date)
    if date_dimension == DimensionName.hour:
        start_ts = from_
        get_next = _next_hour
    elif date_dimension == DimensionName.day:
        start_ts = from_
        get_next = _next_day
    elif date_dimension == DimensionName.week:
        start_ts = from_ - timedelta(days=from_date.weekday())
        get_next = _next_week
    elif date_dimension == DimensionName.month:
        start_ts = from_ - timedelta(days=from_date.day - 1)
        get_next = _next_month
    current_ts = start_ts
    while current_ts < to_:
        yield current_ts
        current_ts = get_next(current_ts)


def collect_stats_v2_raw(g: GitentialContext, workspace_id: int, query: Query) -> QueryResult:
    result = _add_missing_timestamp_to_result(IbisQuery(g, workspace_id, query).execute())
    return result


def collect_stats_v2(g: GitentialContext, workspace_id: int, query: Query):
    if any([m in PR_METRICS for m in query.metrics]) and any(
        [f in [FilterName.author_ids, FilterName.emails, FilterName.team_id] for f in query.filters.keys()]
    ):
        logger.warn("Author based filtering for PRs is not implemented", query=query)
        return {}
    result = collect_stats_v2_raw(g, workspace_id, query)
    return _to_jsonable_result(result)
