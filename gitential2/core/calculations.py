from itertools import product

import datetime as dt
import pandas as pd
import numpy as np
from structlog import get_logger
from gitential2.datatypes.authors import AuthorAlias
from .authors import get_or_create_author_for_alias
from .context import GitentialContext

logger = get_logger(__name__)


def recalculate_repository_values(
    g: GitentialContext, workspace_id: int, repository_id: int
):  # pylint: disable=unused-variable
    logger.info("Recalculating repository commit values", workspace_id=workspace_id, repository_id=repository_id)

    extracted_commits_df, extracted_patches_df, extracted_patch_rewrites_df = g.backend.get_extracted_dataframes(
        workspace_id=workspace_id, repository_id=repository_id
    )
    logger.info(
        "Extracted commits info",
        size=extracted_commits_df.size,
        mem=extracted_commits_df.memory_usage(deep=True).sum(),
    )
    logger.info(
        "Extracted patches info",
        size=extracted_patches_df.size,
        mem=extracted_patches_df.memory_usage(deep=True).sum(),
    )
    logger.info(
        "Extracted patch rewrites info",
        size=extracted_patch_rewrites_df.size,
        mem=extracted_patch_rewrites_df.memory_usage(deep=True).sum(),
    )
    # print(extracted_commits_df, extracted_patches_df, extracted_patch_rewrites_df)
    if extracted_patches_df.empty or extracted_commits_df.empty:
        return None

    parents_df = extracted_patches_df.reset_index()[["commit_id", "parent_commit_id"]].drop_duplicates()

    prepared_commits_df = _prepare_extracted_commits_df(g, workspace_id, extracted_commits_df, parents_df)
    prepared_patches_df = _prepare_extracted_patches_df(extracted_patches_df)
    uploc_df = _calculate_uploc_df(extracted_commits_df, extracted_patch_rewrites_df)

    commits_patches_df = _prepare_commits_patches_df(prepared_commits_df, prepared_patches_df, uploc_df)
    outlier_df = _calc_outlier_detection_df(prepared_patches_df)

    calculated_commits_df = _calculate_commit_level(prepared_commits_df, commits_patches_df, outlier_df)
    calculated_patches_df = _calculate_patch_level(commits_patches_df)

    logger.info("Saving repository commit calculations")

    g.backend.save_calculated_dataframes(
        workspace_id=workspace_id,
        repository_id=repository_id,
        calculated_commits_df=calculated_commits_df,
        calculated_patches_df=calculated_patches_df,
    )
    return prepared_commits_df, prepared_patches_df, commits_patches_df, calculated_commits_df


def _prepare_extracted_commits_df(
    g: GitentialContext, workspace_id: int, extracted_commits_df: pd.DataFrame, parents_df: pd.DataFrame
) -> pd.DataFrame:
    email_author_map = _get_or_create_authors_from_commits(g, workspace_id, extracted_commits_df)
    extracted_commits_df["aid"] = extracted_commits_df.apply(lambda row: email_author_map.get(row["aemail"]), axis=1)
    extracted_commits_df["cid"] = extracted_commits_df.apply(lambda row: email_author_map.get(row["cemail"]), axis=1)
    extracted_commits_df["date"] = extracted_commits_df["atime"]
    extracted_commits_df["is_merge"] = extracted_commits_df["nparents"] > 1
    age_df = _calculate_age_df(extracted_commits_df, parents_df)
    ret = extracted_commits_df.set_index(["commit_id"]).join(age_df)
    hourse_measured_df = _measure_hours(ret)
    return ret.join(hourse_measured_df)


def _prepare_extracted_patches_df(extracted_patches_df: pd.DataFrame) -> pd.DataFrame:
    def _calc_is_test(row):
        return row["langtype"] == "PROGRAMMING" and "test" in row["newpath"]

    extracted_patches_df["is_test"] = extracted_patches_df.apply(_calc_is_test, axis=1)

    extracted_patches_df["outlier"] = 0
    extracted_patches_df["anomaly"] = 0

    return extracted_patches_df.set_index(["commit_id"])


def _calculate_age_df(extracted_commit_df: pd.DataFrame, parents_df: pd.DataFrame) -> pd.DataFrame:
    author_times = extracted_commit_df.set_index(["commit_id"])[["atime"]].to_dict()["atime"]

    def _calc_age(row):
        # print(row["commit_id"], row["parent_commit_id"], author_times.get(row["commit_id"]))
        if row["commit_id"] in author_times and row["parent_commit_id"] in author_times:
            delta = (author_times[row["commit_id"]] - author_times[row["parent_commit_id"]]).total_seconds()
            return delta
        else:
            return -1

    parents_df["age"] = parents_df.apply(_calc_age, axis=1)
    return parents_df.groupby("commit_id").min()["age"].to_frame()


def _calculate_uploc_df(
    extracted_commits_df, extracted_patch_rewrites_df
):  # pylint: disable=compare-to-zero,singleton-comparison
    # prepare patch rewrites
    merges_df = extracted_commits_df[["commit_id", "is_merge"]].set_index("commit_id")
    extracted_patch_rewrites_df = extracted_patch_rewrites_df.join(merges_df, on=["rewritten_commit_id"])
    extracted_patch_rewrites_df = extracted_patch_rewrites_df.join(
        merges_df, on=["commit_id"], rsuffix="__newer", lsuffix="__older"
    )
    # calculate uploc_df
    df = extracted_patch_rewrites_df[
        extracted_patch_rewrites_df["atime"] - extracted_patch_rewrites_df["rewritten_atime"] < dt.timedelta(days=21)
    ]
    df = df[df["is_merge__newer"] == False]
    df = df[df["is_merge__older"] == False]
    uploc_df = (
        pd.DataFrame({"uploc": df.groupby(["rewritten_commit_id", "newpath"])["loc_d"].agg("sum")})
        .reset_index()
        .rename(columns={"rewritten_commit_id": "commit_id"})
        .set_index(["commit_id", "newpath"])
    )
    return uploc_df


def _prepare_commits_patches_df(
    prepared_commits_df: pd.DataFrame, prepared_patches_df: pd.DataFrame, uploc_df: pd.DataFrame
):
    df = (
        prepared_commits_df.join(prepared_patches_df, lsuffix="__commit", rsuffix="__patch")
        .reset_index()
        .set_index(["commit_id", "parent_commit_id", "newpath"])
    )
    df_with_uploc = df.join(uploc_df, on=["commit_id", "newpath"])

    def _finalize_uploc(row):
        if row["is_merge"]:
            return 0
        else:
            return min(row["uploc"], row["loc_i"])

    df_with_uploc["uploc"] = df_with_uploc.apply(_finalize_uploc, axis=1)
    return df_with_uploc


def _get_or_create_authors_from_commits(g: GitentialContext, workspace_id, extracted_commits_df):
    authors_df = (
        extracted_commits_df[["aname", "aemail"]]
        .drop_duplicates()
        .rename(columns={"aname": "name", "aemail": "email"})
        .set_index("email")
    )

    commiters_df = (
        extracted_commits_df[["cname", "cemail"]]
        .drop_duplicates()
        .rename(columns={"cname": "name", "cemail": "email"})
        .set_index("email")
    )

    developers_df = pd.concat([authors_df, commiters_df])
    developers_df = developers_df[~developers_df.index.duplicated(keep="first")]

    authors = [
        get_or_create_author_for_alias(g, workspace_id, AuthorAlias(name=name, email=email))
        for email, name in developers_df["name"].to_dict().items()
    ]
    email_aid_map = {}
    for author in authors:
        for alias in author.aliases:
            email_aid_map[alias.email] = author.id

    return email_aid_map


def _measure_hours(prepared_commits_df: pd.DataFrame) -> pd.DataFrame:
    atime = prepared_commits_df[["aid", "atime"]].sort_values(by=["aid", "atime"], ascending=True).atime

    deltasecs = (atime - atime.shift(1)).dt.total_seconds()
    measured = pd.concat([deltasecs, prepared_commits_df.age], axis=1)
    measured[measured < 0] = np.nan  # substract some sort of overhead
    return (measured.min(axis=1, skipna=True) / 3600).to_frame(name="hours_measured")


def _calculate_commit_level(prepared_commits_df: pd.DataFrame, commits_patches_df: pd.DataFrame, outlier_df):
    calculated_commits = prepared_commits_df.join(
        commits_patches_df.groupby("commit_id")
        .agg({"loc_i": "sum", "loc_d": "sum", "comp_i": "sum", "comp_d": "sum", "uploc": "sum", "aid": "count"})
        .rename(
            columns={
                "aid": "nfiles",
                "loc_i": "loc_i_c",
                "loc_d": "loc_d_c",
                "comp_i": "comp_i_c",
                "comp_d": "comp_d_c",
                "uploc": "uploc_c",
            }
        )
        # commits_patches_df.groupby("commit_id")[["loc_i", "loc_d", "comp_i", "comp_d", "uploc"]].sum()
    )

    calculated_commits["loc_effort_c"] = 1.0 * calculated_commits["loc_i_c"] + 0.2 * calculated_commits["loc_d_c"]

    calculated_commits = calculated_commits.join(outlier_df)

    calculated_commits["velocity_measured"] = calculated_commits["loc_i_c"] / calculated_commits["hours_measured"]
    calculated_commits = _add_estimate_hours(_median_measured_velocity(calculated_commits))
    calculated_commits["velocity"] = calculated_commits["loc_i_c"] / calculated_commits["hours"]
    return calculated_commits


def _calculate_patch_level(calculated_patches_df: pd.DataFrame) -> pd.DataFrame:
    calculated_patches_columns = [
        "repo_id",
        "commit_id",
        "aid",
        "cid",
        "date",
        "parent_commit_id",
        "status",
        "newpath",
        "oldpath",
        "newsize",
        "oldsize",
        "is_binary",
        "lang",
        "langtype",
        "loc_i",
        "loc_d",
        "comp_i",
        "comp_d",
        "nhunks",
        "nrewrites",
        "rewrites_loc",
        "is_merge",
        "is_test",
        "uploc",
        "outlier",
        "anomaly",
    ]

    calculated_patches_df = calculated_patches_df.reset_index().rename(columns={"repo_id__commit": "repo_id"})
    calculated_patches_df = calculated_patches_df[calculated_patches_columns]
    calculated_patches_df = calculated_patches_df[calculated_patches_df["parent_commit_id"].notnull()]
    return calculated_patches_df.set_index(["repo_id", "commit_id", "parent_commit_id", "newpath"])


def _median_measured_velocity(calculated_commits: pd.DataFrame) -> pd.DataFrame:
    accurates = (
        calculated_commits["hours_measured"].between(0.001, 2.0)
        & (calculated_commits["velocity_measured"] > 0)
        & ~calculated_commits["is_merge"]
    )
    medians = calculated_commits[accurates].groupby("aid").agg({"velocity_measured": "median"})
    medians.columns = ["median_velocity_measured"]
    df = calculated_commits.merge(medians, left_on="aid", right_index=True)
    return df


def _add_estimate_hours(calculated_commits: pd.DataFrame) -> pd.DataFrame:
    df = calculated_commits.copy()
    df["hours_estimated"] = (df["loc_i_c"] / df["median_velocity_measured"]).fillna(1 / 12)
    df["hours_estimated"] = df["hours_estimated"].clip(lower=1 / 12)
    df["hours"] = df[["hours_measured", "hours_estimated"]].min(axis=1, skipna=True).clip(upper=4.0)
    return df


def _calc_outlier_detection_df(prepared_patches_df: pd.DataFrame) -> pd.DataFrame:
    pdf = prepared_patches_df.copy()
    columns = [
        "loc_i",
        "loc_d",
        "comp_i",
        "comp_d",
    ]  # 'blame_loc'

    stats = pdf.reset_index().groupby(["commit_id", "outlier"])[columns].sum().unstack(fill_value=0)

    stats.columns = [
        "{}_{}".format(metric, "outlier" if outlier else "inlier") for metric, outlier in stats.columns.values
    ]

    required_columns = map("_".join, product(columns, ["inlier", "outlier"]))
    for col in required_columns:
        if col not in stats:
            stats[col] = 0
    return stats
