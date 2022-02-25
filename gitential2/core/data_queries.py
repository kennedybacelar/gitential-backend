from typing import List, Tuple, cast
from ibis.expr.types import TableExpr, ColumnExpr
import pandas as pd
from gitential2.datatypes.data_queries import (
    DQColumnAttrName,
    DQColumnExpr,
    DQFnColumnExpr,
    DQFunctionName,
    DQSelectionExpr,
    DQSingleColumnExpr,
    DQSortByExpr,
    DQSourceName,
    MultiQuery,
    DataQuery,
    DQFilterExpr,
    DQType,
    DQDimensionExpr,
    DQ_ITS_SOURCE_NAMES,
)
from .context import GitentialContext


# def process_data_query(g: GitentialContext, workspace_id: int, query: DataQuery):
#     result, total = execute_data_query()


def execute_multi_query(g: GitentialContext, workspace_id: int, query: MultiQuery) -> dict:
    return {k: execute_data_query(g, workspace_id, v) for k, v in query.items()}


def execute_data_query(g: GitentialContext, workspace_id: int, query: DataQuery) -> Tuple[pd.DataFrame, int]:
    query = _simplify_query(g, workspace_id, query)
    _, ibis_query = parse_data_query(g, workspace_id, query)

    total_count = None
    if query.limit is not None and query.offset is not None:
        total_count = ibis_query.count().execute()
        ibis_query = ibis_query.limit(query.limit, offset=query.offset)

    result: pd.DataFrame = ibis_query.execute()
    if total_count is None:
        total_count = len(result.index)
    return result, total_count


def parse_data_query(g: GitentialContext, workspace_id: int, query: DataQuery) -> TableExpr:
    table = g.backend.get_ibis_table(workspace_id, query.source_name.value)
    filters = _prepare_filters(query.filters, table)
    selections = _prepare_selections(query.selections, table)
    dimensions = _prepare_dimensions(query.dimensions, table)
    sorty_by_s = _prepare_sort_by_s(query.sort_by, table)
    ibis_query = _construct_ibis_query(query.query_type, table, filters, selections, dimensions, sorty_by_s)
    return table, ibis_query


def _simplify_query(g: GitentialContext, workspace_id: int, query: DataQuery):
    # replace project_id to itsp_id or repo_id
    # replace team_id to dev_id
    query.filters = [_simplify_filter(g, workspace_id, f, query.source_name) for f in query.filters]
    return query


def _simplify_filter(
    g: GitentialContext, workspace_id: int, f: DQFilterExpr, source_name: DQSourceName
) -> DQFilterExpr:
    if f.args:
        if f.fn == DQFunctionName.EQ and isinstance(f.args[0], DQSingleColumnExpr) and f.args[0].col == "project_id":
            project_id = int(cast(int, f.args[1]))
            if source_name in DQ_ITS_SOURCE_NAMES:
                itsp_ids = g.backend.project_its_projects.get_itsp_ids_for_project(workspace_id, project_id)
                return DQFnColumnExpr(fn=DQFunctionName.IN, args=[DQSingleColumnExpr(col="itsp_id"), itsp_ids])
            else:
                repo_ids = g.backend.project_repositories.get_repo_ids_for_project(workspace_id, project_id)
                return DQFnColumnExpr(fn=DQFunctionName.IN, args=[DQSingleColumnExpr(col="repo_id"), repo_ids])
    return f


def _construct_ibis_query(
    query_type: DQType,
    table: TableExpr,
    filters: list,
    metrics: list,
    dimensions: list,
    sort_by_s: list,
):
    ibis_query = table.filter(filters) if filters else table
    if query_type == DQType.select:
        ibis_query = ibis_query.select(metrics)
    elif query_type == DQType.aggregate:
        ibis_query = ibis_query.group_by(dimensions).aggregate(metrics)
    if sort_by_s:
        ibis_query = ibis_query.sort_by(sort_by_s)
    return ibis_query


def _prepare_filters(filters: List[DQFilterExpr], table: TableExpr) -> list:
    return [_parse_column_expr(filter_, table) for filter_ in filters or []]


def _prepare_selections(selections: List[DQSelectionExpr], table: TableExpr) -> list:
    return [_parse_column_expr(selection, table) for selection in selections]


def _prepare_dimensions(dimensions: List[DQDimensionExpr], table: TableExpr) -> list:
    return [_parse_column_expr(dimension, table) for dimension in dimensions or []]


def _prepare_sort_by_s(sort_by_s: List[DQSortByExpr], table: TableExpr):
    return [_prepare_sort_by(sort_by_expr, table) for sort_by_expr in sort_by_s or []]


# pylint: disable=unused-argument
def _prepare_sort_by(sort_by_expr: DQSortByExpr, table: TableExpr) -> Tuple[str, bool]:
    return (sort_by_expr.col, not sort_by_expr.desc)


def _parse_column_expr(column_expr: DQColumnExpr, table: TableExpr) -> ColumnExpr:
    if isinstance(column_expr, DQSingleColumnExpr):
        return _parse_single_column_expr(column_expr, table)
    elif isinstance(column_expr, DQFnColumnExpr):
        return _parse_fn_column_expr(column_expr, table)
    else:
        # static value, just return with it
        return column_expr


def _parse_single_column_expr(column_expr: DQSingleColumnExpr, table: TableExpr):
    ret = table[column_expr.col]
    if column_expr.attr:
        ret = _parse_attr(ret, column_expr.attr)
    if column_expr.as_:
        ret = ret.name(column_expr.as_)
    return ret


def _parse_fn_column_expr(column_expr: DQFnColumnExpr, table: TableExpr):
    fn_name = column_expr.fn
    parsed_args = [_parse_column_expr(arg, table) for arg in column_expr.args]

    fn_definitions = {
        # aggregations
        DQFunctionName.MEAN: lambda pa: pa[0].mean(),
        DQFunctionName.SUM: lambda pa: pa[0].sum(),
        DQFunctionName.COUNT: lambda pa: pa[0].count() if pa else table.count(),
        # filtering
        DQFunctionName.EQ: lambda pa: pa[0] == pa[1],
        DQFunctionName.NEQ: lambda pa: pa[0] != pa[1],
        DQFunctionName.LT: lambda pa: pa[0] < pa[1],
        DQFunctionName.LTE: lambda pa: pa[0] <= pa[1],
        DQFunctionName.GT: lambda pa: pa[0] > pa[1],
        DQFunctionName.GTE: lambda pa: pa[0] >= pa[1],
        DQFunctionName.IN: lambda pa: pa[0].isin(pa[1]),
        DQFunctionName.NIN: lambda pa: pa[0].notin(pa[1]),
        DQFunctionName.BETWEEN: lambda pa: pa[0].between(pa[1], pa[2]),
        # mathematical operations
        DQFunctionName.MUL: lambda pa: pa[0] * pa[1],
        DQFunctionName.DIV: lambda pa: pa[0] / pa[1],
        DQFunctionName.ADD: lambda pa: pa[0] + pa[1],
        DQFunctionName.SUB: lambda pa: pa[0] - pa[1],
    }
    ret = fn_definitions[fn_name](parsed_args)

    if column_expr.attr:
        ret = _parse_attr(ret, column_expr.attr)

    if column_expr.as_:
        ret = ret.name(column_expr.as_)
    return ret


def _parse_attr(expr, attr_name: DQColumnAttrName):
    predefined_attrs = {
        DQColumnAttrName.ROUND_TO_DAY: lambda col: col.date(),
        DQColumnAttrName.ROUND_TO_WEEK: lambda col: col.date().truncate("W"),
        DQColumnAttrName.ROUND_TO_MONTH: lambda col: col.date().truncate("M"),
        DQColumnAttrName.ROUND_TO_HOUR: lambda col: col.truncate("H"),
        DQColumnAttrName.TO_DAY_OF_WEEK: lambda col: col.date().day_of_week.index(),
        DQColumnAttrName.TO_HOUR_OF_DAY: lambda col: col.hour(),
        DQColumnAttrName.EPOCH_SECONDS: lambda col: col.epoch_seconds(),
    }
    if attr_name in predefined_attrs:
        return predefined_attrs[attr_name](expr)
    else:
        attr = getattr(expr, attr_name.value)
        if callable(attr):
            return attr()
        else:
            return attr
