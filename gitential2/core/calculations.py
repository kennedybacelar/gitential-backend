import pandas as pd

from gitential2.datatypes.authors import AuthorAlias
from .authors import get_or_create_author_for_alias
from .context import GitentialContext


def recalculate_repository_values(g: GitentialContext, workspace_id: int, repository_id: int):
    extracted_commits_df, extracted_patches_df, extracted_patch_rewrites_df = g.backend.get_extracted_dataframes(
        workspace_id=workspace_id, repository_id=repository_id
    )
    # print(extracted_commits_df, extracted_patches_df, extracted_patch_rewrites_df)

    email_author_map = _get_or_create_authors_from_commits(g, workspace_id, extracted_commits_df)

    calculated_commits_df = extracted_commits_df.copy()
    calculated_commits_df["date"] = calculated_commits_df["atime"]
    calculated_commits_df["aid"] = calculated_commits_df.apply(lambda row: email_author_map.get(row["aemail"]), axis=1)
    calculated_commits_df["cid"] = calculated_commits_df.apply(lambda row: email_author_map.get(row["cemail"]), axis=1)

    calculated_patches_df = extracted_patches_df.copy()

    # print(calculated_commits_df[["aemail", "aid", "cemail", "cid"]])

    g.backend.save_calculated_dataframes(
        workspace_id=workspace_id,
        repository_id=repository_id,
        calculated_commits_df=calculated_commits_df,
        calculated_patches_df=calculated_patches_df,
    )


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
