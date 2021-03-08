import json
import urllib
import os

import requests
import ibis
import pandas as pd
from ibis.backends.postgres import connect

from gitential2.settings import GitentialSettings
from gitential2.datatypes.stats import StatsRequest

from .context import GitentialContext

INT64 = "int64"
BOOLEAN = "boolean"
STRING = "string"
TIMESTAMP = "timestamp"

pull_requests = ibis.table(
    [
        ("repo_id", INT64),
        ("number", INT64),
        ("title", STRING),
        ("platform", STRING),
        ("id_platform", INT64),
        ("api_resource_uri", STRING),
        ("state_platform", STRING),
        ("state", STRING),
        ("created_at", TIMESTAMP),
        ("closed_at", TIMESTAMP),
        ("updated_at", TIMESTAMP),
        ("merged_at", TIMESTAMP),
        ("additions", INT64),
        ("deletions", INT64),
        ("changed_files", INT64),
        ("draft", BOOLEAN),
        ("user", STRING),
        ("commits", INT64),
        ("merged_by", STRING),
        ("first_reaction_at", TIMESTAMP),
        ("first_commit_authored_at", TIMESTAMP),
    ],
    "pull_requests",
)

metrics = {"sum_pr_count": pull_requests.count()}


def _ws_schema(workspace_id):
    return f"ws_{workspace_id}"


def _fix_day_filter(q):
    def _fix(day):
        if isinstance(day, str):
            return day.split("T")[0]
        else:
            return day

    if "day" in q["filters"]:
        q["filters"]["day"] = [_fix(day) for day in q["filters"]["day"]]


def collect_stats(g: GitentialContext, workspace_id: int, request: StatsRequest):
    return None


def calculate_stats(request: StatsRequest, workspace_id: int, settings: GitentialSettings):
    # conn = connect(url=settings.connections.database_url)
    # # conn.execute(conn.schema(_ws_schema(workspace_id)).pull_requests.count())
    # expression = metrics["sum_pr_count"]
    # print(dir(expression), expression._root_tables)

    PR_METRICS = [
        "avg_pr_commit_count",
        "avg_pr_code_volume",
        "avg_review_time",
        "avg_pickup_time",
        "avg_development_time",
        "pr_merge_ratio",
        "sum_pr_closed",
        "sum_pr_merged",
        "sum_review_comment_count",
        "avg_pr_review_comment_count",
        "sum_pr_count",
        "avg_pr_cycle_time",
    ]

    # # conn.execute(metrics["sum_pr_count"], schema=_ws_schema(workspace_id))
    print(request)
    q = {
        "metrics": [m for m in request.metrics if m not in PR_METRICS],
        "dimensions": request.dimensions,
        "filters": request.filters,
    }

    if "account_id" in q["filters"]:
        q["filters"]["account_id"] = 2319
    if "project_id" in q["filters"]:
        q["filters"]["project_id"] = 4329

    if "is_merge" in q["filters"]:
        q["filters"]["ismerge"] = q["filters"]["is_merge"]
        del q["filters"]["is_merge"]

    if not q["dimensions"]:
        del q["dimensions"]

    _fix_day_filter(q)

    if q["metrics"]:
        # checking the legacy API
        q_str = urllib.parse.quote(json.dumps(q))
        q_filename = "/tmp/" + "".join(
            ch
            for ch in json.dumps(q).replace("metrics", "M").replace("dimensions", "D").replace("filters", "F")
            if ch.isalnum()
        )
        if os.path.isfile(q_filename):
            result = json.loads(open(q_filename, "r").read())
        else:
            # print(request.metrics)
            # # print(conn.table("pull_requests", schema=_ws_schema(workspace_id)).count())
            # # print(conn.list_tables(), conn.list_schemas())
            response = requests.get(
                f"https://api.gitential.com/stats?q={q_str}&type={request.type}",
                headers={
                    "Cookie": "_ga=GA1.2.105986898.1602805349; _hjid=8997894d-bc95-4851-a752-8744f9579c7d; hubspotutk=e34a7503d436ed4c0f69c63166ca3446; session=16f7e898a6d948fbb8101fa3eaa227f3; tk_or=%22https%3A%2F%2Fapp.gitential.com%2F%22; messagesUtk=c640caba32cb4fe6b9179c5db94a871b; tk_ai=iSQvbSnYk4qtM4pBneNTKPao; __stripe_mid=405c7f91-11de-4827-98b2-34cb56df398d308f6f; _fbp=fb.1.1607691142497.649903801; tk_lr=%22%22; tk_r3d=%22%22; _gid=GA1.2.1510749756.1613610571; _hjTLDTest=1; _hjAbsoluteSessionInProgress=0; __hstc=80255432.e34a7503d436ed4c0f69c63166ca3446.1602805352108.1613490270197.1613610575135.141; __hssrc=1; fs_uid=rs.fullstory.com#9VVTJ#5410472100347904:5633306881933312#aeb65a60#/1645147082; _gat_UA-85999832-1=1; __hssc=80255432.9.1613610575135"
                },
                verify=False,
            )
            if response.status_code == 500:
                print(response.text)
                result = {}
            else:
                result = response.json()
                with open(q_filename, "w") as f:
                    f.write(json.dumps(result))
        return result

    else:
        pr_m_needed = [metric for metric in request.metrics if metric in PR_METRICS]
        if pr_m_needed:
            result = _calculate_pr_metrics(pr_m_needed, request, settings, workspace_id)
        return result
        # keys = request.metrics + (request.dimensions or [])
        # if request.sort_by:
        #     keys = keys + request.sort_by
        # return {key: [] for key in keys}


def _calculate_pr_metrics(metric_names, request, settings, workspace_id):
    conn = connect(url=settings.connections.database_url)
    prs = conn.schema(_ws_schema(workspace_id)).pull_requests
    day_filter_ = request.filters.get("day", ["2019-01-01", "2021-02-18"])
    day_filters = [prs["created_at"] > day_filter_[0], prs["created_at"] < day_filter_[1]]
    filters = day_filters
    sort_by = [prs["created_at"]]
    metrics = {
        "sum_pr_count": prs.count().name("sum_pr_count"),
        # "pr_merge_ratio": (prs.filter(prs["state"] == "merged").count()).name("pr_merge_ratio"),
        # "sum_pr_closed": prs["state"].value_counts()["state"].name("sum_pr_closed"),
        # "sum_pr_merged": prs[prs["state"] == "merged"].count().name("sum_pr_merged"),
        "avg_pr_commit_count": prs["commits"].mean().name("avg_pr_commit_count"),
        "avg_pr_code_volume": prs["additions"].mean().name("avg_pr_code_volume"),
        "avg_pr_cycle_time": (
            (prs["merged_at"].epoch_seconds() - prs["first_commit_authored_at"].epoch_seconds()).abs() / 3600
        )
        .mean()
        .name("avg_pr_cycle_time"),
        "avg_review_time": ((prs["merged_at"].epoch_seconds() - prs["first_reaction_at"].epoch_seconds()).abs() / 3600)
        .mean()
        .name("avg_review_time"),
        "avg_pickup_time": ((prs["first_reaction_at"].epoch_seconds() - prs["created_at"].epoch_seconds()).abs() / 3600)
        .mean()
        .name("avg_pickup_time"),
        "avg_development_time": (
            (prs["created_at"].epoch_seconds() - prs["first_commit_authored_at"].epoch_seconds()).abs() / 3600
        )
        .mean()
        .name("avg_development_time"),
        "sum_review_comment_count": prs["commits"].sum().name("sum_review_comment_count"),
        "avg_pr_review_comment_count": prs["commits"].mean().name("avg_pr_review_comment_count"),
    }
    metrics_needed = []
    metrics_missed = []

    for m_name in metric_names:
        if m_name in metrics:
            metrics_needed.append(metrics[m_name])
        else:
            metrics_missed.append(m_name)

    if request.type != "aggregate":

        query = prs.filter(filters).sort_by(sort_by).select(metrics_needed)
        print(conn.execute(query))
        return conn.execute(query)
    else:
        print("DIMENSIONS", request.dimensions)
        dimensions = request.dimensions or []
        dimensions_ = []
        needs_sorting = False
        if "day" in dimensions:
            dimensions_.append((prs["created_at"].date().epoch_seconds() * 1000).name("date"))
            needs_sorting = True
        elif "week" in dimensions:
            dimensions_.append((prs["created_at"].date().truncate("W").epoch_seconds() * 1000).name("date"))
            needs_sorting = True
        elif "month" in dimensions:
            dimensions_.append((prs["created_at"].date().truncate("M").epoch_seconds() * 1000).name("date"))
            needs_sorting = True

        query = prs.aggregate(metrics=metrics_needed, by=dimensions_).filter(filters)
        if needs_sorting:
            query = query.sort_by("date")
        result = query.execute()

        if "sum_pr_closed" in metrics_missed:
            sum_pr_closed_query = (
                prs.filter(prs["state"] == "closed")
                .filter(filters)
                .aggregate(metrics=[prs.count().name("sum_pr_closed")], by=dimensions_)
            )
            if needs_sorting:
                sum_pr_closed_query = sum_pr_closed_query.sort_by("date")

            sum_pr_closed_df = sum_pr_closed_query.execute()
            sum_pr_closed_df = sum_pr_closed_df.fillna(0)
            result["sum_pr_closed"] = sum_pr_closed_df["sum_pr_closed"]

        if "sum_pr_merged" in metrics_missed:
            pr_merged_counts_query = (
                prs.filter(prs["state"] == "merged")
                .filter(filters)
                .aggregate(metrics=[prs.count().name("merged_count")], by=dimensions_)
            )
            if needs_sorting:
                pr_merged_counts_query = pr_merged_counts_query.sort_by("date")

            pr_merged_counts_df = pr_merged_counts_query.execute()
            result["sum_pr_merged"] = pr_merged_counts_df["merged_count"]

        if "pr_merge_ratio" in metrics_missed:
            pr_merged_counts_query = (
                prs.filter(prs["state"] == "merged")
                .filter(filters)
                .aggregate(metrics=[prs.count().name("merged_count")], by=dimensions_)
            )
            if needs_sorting:
                pr_merged_counts_query = pr_merged_counts_query.sort_by("date")

            pr_merged_counts_df = pr_merged_counts_query.execute()

            pr_counts_query = prs.aggregate(metrics=[prs.count().name("total_count")], by=dimensions_).filter(filters)
            if needs_sorting:
                pr_counts_query = pr_counts_query.sort_by("date")

            pr_counts_df = pr_counts_query.execute()

            merge_ratio_df = pd.concat([pr_merged_counts_df, pr_counts_df], axis=1)
            merge_ratio_df["merge_ratio"] = merge_ratio_df["merged_count"] / merge_ratio_df["total_count"] * 100
            result["pr_merge_ratio"] = merge_ratio_df["merge_ratio"]

        print("*" * 100)
        print(request)
        print(result)
        print("*" * 100)
        result = result.fillna(0)
        return result.to_dict(orient="list")
