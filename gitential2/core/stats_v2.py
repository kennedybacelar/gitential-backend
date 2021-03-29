from typing import List, Optional, Any, Dict, Union
from structlog import get_logger
from pydantic import BaseModel
import pandas as pd

from gitential2.datatypes.stats import IbisTables, StatsRequest, MetricName, DimensionName, FilterName, TableName
from gitential2.exceptions import InvalidStateException
from .context import GitentialContext


logger = get_logger(__name__)

METRICS_BY_TABLES: Dict[TableName, List[MetricName]] = {
    TableName.pull_requests: [
        MetricName.avg_pr_commit_count,
        MetricName.avg_pr_code_volume,
        MetricName.avg_review_time,
        MetricName.avg_pickup_time,
        MetricName.avg_development_time,
        MetricName.pr_merge_ratio,
        MetricName.sum_pr_closed,
        MetricName.sum_pr_merged,
        MetricName.sum_pr_open,
        MetricName.sum_review_comment_count,
        MetricName.avg_pr_review_comment_count,
        MetricName.sum_pr_count,
        MetricName.avg_pr_cycle_time,
    ],
    TableName.calculated_commits: [
        MetricName.count_commits,
        MetricName.sum_loc_effort,
        MetricName.sum_hours,
        MetricName.sum_ploc,
        MetricName.efficiency,
        MetricName.nunique_contributors,
    ],
    TableName.calculated_patches: [],
}


class StatQuery(BaseModel):
    table: TableName
    metrics: List[MetricName]
    dimensions: Optional[List[DimensionName]] = None
    filters: Dict[FilterName, Any]
    sort_by: Optional[List[Union[str, int]]] = None
    type: str = "aggregate"  # or "select"


class QueryResult(BaseModel):
    query: StatQuery
    values: pd.DataFrame

    class Config:
        arbitrary_types_allowed = True


def _get_table_for_metric(m: MetricName) -> TableName:
    for table_name, metrics in METRICS_BY_TABLES.items():
        if m in metrics:
            return table_name
    raise InvalidStateException(f"No table defined for metric: {m}")


def _request_to_queries(request: StatsRequest) -> List[StatQuery]:
    queries = {}
    for metric in request.metrics:
        table_name = _get_table_for_metric(metric)
        if table_name not in queries:
            queries[table_name] = StatQuery(
                table=table_name,
                metrics=[metric],
                dimensions=request.dimensions,
                filters=request.filters,
                sort_by=request.sort_by,
                type=request.type,
            )
        else:
            queries[table_name].metrics.append(metric)
    return list(queries.values())


def _prepare_dimensions(dimensions, table_name: TableName, ibis_table):
    ret = []
    for dimension in dimensions:
        res = _prepare_dimension(dimension, table_name, ibis_table)
        if res is not None:
            ret.append(res)
    return ret


def _prepare_dimension(dimension: DimensionName, table_name: TableName, ibis_tables: IbisTables):
    ibis_table = ibis_tables.get_table(table_name)
    if table_name == TableName.pull_requests:
        date_field_name = "created_at"
    else:
        date_field_name = "date"
    if dimension == DimensionName.day:
        return (ibis_table[date_field_name].date().epoch_seconds() * 1000).name("date")
    elif dimension == DimensionName.week:
        return (ibis_table[date_field_name].date().truncate("W").epoch_seconds() * 1000).name("date")
    elif dimension == DimensionName.month:
        return (ibis_table[date_field_name].date().truncate("M").epoch_seconds() * 1000).name("date")
    elif dimension == DimensionName.pr_state and table_name == TableName.pull_requests:
        return ibis_tables.pull_requests.state.name("pr_state")
    if table_name == TableName.calculated_patches:
        if dimension == DimensionName.language:
            return ibis_tables.patches.language.name("language")
    return None


def _prepare_metrics(metrics, table_name: TableName, ibis_tables):
    ret = []
    for metric in metrics:
        if table_name == TableName.calculated_commits:
            res = _prepare_commits_metric(metric, ibis_tables)
        elif table_name == TableName.pull_requests:
            res = _prepare_prs_metric(metric, ibis_tables)
        if res is not None:
            ret.append(res)
    return ret


def _prepare_commits_metric(metric: MetricName, ibis_tables: IbisTables):
    t = ibis_tables

    # commit metrics
    count_commits = t.commits.count().name("count_commits")
    loc_effort = t.commits.loc_effort.sum().name("sum_loc_effort")
    sum_hours = t.commits.hours.sum().name("sum_hours")
    sum_ploc = (t.commits.loc_i.sum() - t.commits.uploc.sum()).name("sum_ploc")
    efficiency = (sum_ploc / t.commits.loc_i.sum() * 100).name("efficiency")
    nunique_contributors = t.commits.aid.nunique().name("nunique_contributors")

    commit_metrics = {
        MetricName.count_commits: count_commits,
        MetricName.sum_loc_effort: loc_effort,
        MetricName.sum_hours: sum_hours,
        MetricName.sum_ploc: sum_ploc,
        MetricName.efficiency: efficiency,
        MetricName.nunique_contributors: nunique_contributors,
    }

    return commit_metrics.get(metric)


def _prepare_prs_metric(metric: MetricName, ibis_tables: IbisTables):

    prs = ibis_tables.pull_requests

    sum_pr_count = prs.count().name("sum_pr_count")
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

    pr_metrics = {
        # "pr_merge_ratio": (prs.filter(prs["state"] == "merged").count()).name("pr_merge_ratio"),
        # "sum_pr_closed": prs["state"].value_counts()["state"].name("sum_pr_closed"),
        # "sum_pr_merged": prs[prs["state"] == "merged"].count().name("sum_pr_merged"),
        MetricName.sum_pr_count: sum_pr_count,
        MetricName.avg_pr_commit_count: avg_pr_commit_count,
        MetricName.avg_pr_code_volume: avg_pr_code_volume,
        MetricName.avg_pr_cycle_time: avg_pr_cycle_time,
        MetricName.avg_review_time: avg_review_time,
        MetricName.avg_pickup_time: avg_pickup_time,
        MetricName.avg_development_time: avg_development_time,
        MetricName.sum_review_comment_count: sum_review_comment_count,
        MetricName.avg_pr_review_comment_count: avg_pr_review_comment_count,
    }
    return pr_metrics.get(metric)


def _prepare_filters(  # pylint: disable=too-complex
    g: GitentialContext,
    workspace_id: int,
    filters: Dict[FilterName, Any],
    table_name: TableName,
    ibis_tables: IbisTables,
) -> list:
    filters_dict: dict = {}

    t = ibis_tables

    _ibis_filters: dict = {
        TableName.calculated_commits: {
            FilterName.repo_ids: t.commits.repo_id.isin,
            FilterName.emails: t.commits.aemail.isin,
            "aids": t.commits.aid.isin,
            "name": t.commits.aname.isin,
            FilterName.day: t.commits.date.between,
            FilterName.is_merge: t.commits.is_merge.__eq__,
            "keyword": t.commits.message.lower().re_search,
            "outlier": t.patches.outlier.__eq__,
            "commit_msg": t.commits.message.lower().re_search,
        },
        TableName.pull_requests: {
            FilterName.repo_ids: t.pull_requests.repo_id.isin,
            FilterName.day: t.pull_requests.created_at.between,
        },
    }

    for filter_name, filter_params in filters.items():
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
        elif filter_name == FilterName.day:
            filters_dict[FilterName.day] = filter_params
        elif filter_name == FilterName.is_merge:
            filters_dict[FilterName.is_merge] = filter_params

    ret = []
    for filter_key, values in filters_dict.items():
        if filter_key in _ibis_filters.get(table_name, {}):
            filter_ = _ibis_filters[table_name][filter_key]
            if filter_.__name__ == "isin":
                ret.append(filter_(values))
            elif isinstance(values, list):
                ret.append(filter_(*values))
            else:
                ret.append(filter_(values))
    return ret


def _prepare_sort_by(query: StatQuery):
    if (
        not set({DimensionName.day, DimensionName.week, DimensionName.month, DimensionName}).isdisjoint(
            set(query.dimensions or [])
        )
        and not query.sort_by
    ):
        print("adding date sort_by", query.dimensions, query.sort_by)
        return ["date"]
    else:
        return query.sort_by


def _exec_query(
    g: GitentialContext,
    workspace_id: int,
    query: StatQuery,
):
    logger.debug("Executing query", query=query, workspace_id=workspace_id)
    ibis_tables = g.backend.get_ibis_tables(workspace_id)
    ibis_table = ibis_tables.get_table(query.table)
    ibis_metrics = _prepare_metrics(query.metrics, query.table, ibis_tables)
    ibis_dimensions = _prepare_dimensions(query.dimensions, query.table, ibis_tables) if query.dimensions else None
    ibis_filters = _prepare_filters(g, workspace_id, query.filters, query.table, ibis_tables)

    # print("IBIS_METRICS", ibis_metrics)
    # print("*" * 120)
    # print("IBIS_DIMENSIONS", ibis_dimensions)
    # print("*" * 120)
    # print("IBIS_FILTERS", ibis_filters)
    # print("*" * 120)

    if ibis_metrics:
        if query.type == "aggregate":
            # ibis_table.aggregate(ibis_metrics, by=query.dimensions).filter(query.filters)
            ibis_query = ibis_table.filter(ibis_filters).aggregate(metrics=ibis_metrics, by=ibis_dimensions)
        else:
            ibis_query = ibis_table.filter(ibis_filters).select(ibis_metrics)

        result = ibis_tables.conn.execute(ibis_query)
    else:
        result = pd.DataFrame()

    print("RESULT", result)

    sort_by = _prepare_sort_by(query)
    if sort_by:
        print("SORTING", result.columns, sort_by, [s for s in sort_by if s in result.columns])
        result = result.sort_values(by=[s for s in sort_by if s in result.columns])

    return QueryResult(query=query, values=result)
    # print(ibis_table)


def _merge_query_results(results: List[QueryResult]):
    for r in results:
        ret = r.values.fillna(0)
        print("INDEX", ret.index)
        return ret.to_dict(orient="list")


def collect_stats_v2(g: GitentialContext, workspace_id: int, request: StatsRequest):
    queries = _request_to_queries(request)
    results = [_exec_query(g, workspace_id, query) for query in queries]

    return _merge_query_results(results)
