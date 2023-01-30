import argparse
import csv
import subprocess
from datetime import date, datetime
from json import loads
from typing import List

import pandas


def _log(msg):
    print(f"{datetime.utcnow().isoformat()} {msg}")


def _simplify_user_data(user_data):
    ret = {}
    for k, v in user_data["user"].items():
        ret[f"user_{k}"] = v
    for k, v in user_data["subscription"].items():
        ret[f"subscription_{k}"] = v
    for k, v in user_data["sum"].items():
        ret[k] = v
    ret["workspace_ids"] = ",".join([workspace_id for workspace_id in user_data["workspace_stats"].keys()])
    ret["workspace_count"] = len(list(user_data["workspace_stats"].keys()))
    return ret


def create_usage_stat_csvs_and_xls(file_name_base_with_abs_path) -> List[str]:
    file_names: List[str] = []

    with open(f"{file_name_base_with_abs_path}.json", "r", encoding="utf-8") as usage_file:
        usage_data = loads(usage_file.read())
    _log(f"Loaded usage data from {file_name_base_with_abs_path}.json")

    result = []
    for user in usage_data:
        result.append(_simplify_user_data(user))

    result_workspace = []
    for user in usage_data:
        for _, workspace_data in user["workspace_stats"].items():
            w = {}
            for k, v in user["user"].items():
                if k in ["id", "login"]:
                    w[f"user_{k}"] = v
            for k, v in workspace_data.items():
                w[k] = v

            result_workspace.append(w)

    usage_stats_file_name_csv = f"{file_name_base_with_abs_path}.csv"
    usage_stats_file_name_xls = f"{file_name_base_with_abs_path}.xls"
    with open(usage_stats_file_name_csv, "w", encoding="utf-8") as csvfile:
        fieldnames = [
            "user_id",
            "workspace_ids",
            "workspace_count",
            "user_login",
            "user_email",
            "user_is_admin",
            "user_marketing_consent_accepted",
            "user_first_name",
            "user_last_name",
            "user_company_name",
            "user_position",
            "user_development_team_size",
            "user_registration_ready",
            "user_login_ready",
            "user_is_active",
            "user_created_at",
            "user_updated_at",
            "user_last_interaction_at",
            "user_stripe_customer_id",
            "subscription_stripe_subscription_id",
            "subscription_user_id",
            "subscription_subscription_start",
            "subscription_subscription_end",
            "subscription_subscription_type",
            "subscription_number_of_developers",
            "subscription_features",
            "subscription_created_at",
            "subscription_updated_at",
            "subscription_id",
            "total_repos_count",
            "total_committers_30days",
            "total_committers_90days",
            "private_repos_count",
            "private_committers_30days",
            "private_committers_90days",
            "public_repos_count",
            "public_committers_30days",
            "public_committers_90days",
            "github_repos_count",
            "github_committers_30days",
            "github_committers_90days",
            "bitbucket_repos_count",
            "bitbucket_committers_30days",
            "bitbucket_committers_90days",
            "vsts_repos_count",
            "vsts_committers_30days",
            "vsts_committers_90days",
            "gitlab_repos_count",
            "gitlab_committers_30days",
            "gitlab_committers_90days",
            "authors_with_active_flag_count",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for r in result:
            _log(f"Writing a row to {usage_stats_file_name_csv}")
            writer.writerow(r)

        file_names.append(usage_stats_file_name_csv)

        pandas.read_csv(usage_stats_file_name_csv).to_excel(usage_stats_file_name_xls, index=None, header=True)
        file_names.append(usage_stats_file_name_csv)

    usage_stats_workspaces_file_name_csv = f"{file_name_base_with_abs_path}_workspaces.csv"
    usage_stats_workspaces_file_name_xls = f"{file_name_base_with_abs_path}_workspaces.xls"
    with open(usage_stats_workspaces_file_name_csv, "w", encoding="utf-8") as csvfile:
        fieldnames = [
            "user_id",
            "workspace_id",
            "user_login",
            "workspace_name",
            "total_repos_count",
            "total_committers_30days",
            "total_committers_90days",
            "private_repos_count",
            "private_committers_30days",
            "private_committers_90days",
            "public_repos_count",
            "public_committers_30days",
            "public_committers_90days",
            "github_repos_count",
            "github_committers_30days",
            "github_committers_90days",
            "bitbucket_repos_count",
            "bitbucket_committers_30days",
            "bitbucket_committers_90days",
            "vsts_repos_count",
            "vsts_committers_30days",
            "vsts_committers_90days",
            "gitlab_repos_count",
            "gitlab_committers_30days",
            "gitlab_committers_90days",
            "authors_with_active_flag_count",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for r in result_workspace:
            r_ = {f: r[f] for f in fieldnames}
            _log(f"Writing a row to {file_name_base_with_abs_path}_workspaces.csv")
            writer.writerow(r_)

        file_names.append(usage_stats_workspaces_file_name_csv)

        pandas.read_csv(usage_stats_workspaces_file_name_csv).to_excel(
            usage_stats_workspaces_file_name_xls, index=None, header=True
        )
        file_names.append(usage_stats_workspaces_file_name_xls)

    return file_names


def get_usage_stats(output_file: str, environment: str):
    kubectl_cmd = f"kubectl -n {environment} exec -ti deployment.apps/gitential-gitential-web -c backend -- poetry run python -m gitential2 usage-stats global"
    _log(f"Running command: {kubectl_cmd}")
    output = subprocess.check_output(kubectl_cmd.split())
    if output:
        with open(output_file, "w", encoding="utf-8") as o:
            _log(f"Saving file: {output_file}")
            o.write(output.decode("utf-8"))
        return output.decode("utf-8")
    else:
        return ""


def upload_files(file_names: List[str], bucket_name: str, environment: str):
    for file in file_names:
        upload_cmd = f"aws s3 cp {file} s3://{bucket_name}/{environment}/usage-stats/"
        subprocess.call(upload_cmd.split())
        _log(f"Uploaded {file} to s3://{bucket_name}/{environment}/usage-stats/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect and upload daily usage stats")
    parser.add_argument("-d", "--date", default=date.today().isoformat())
    parser.add_argument("-w", "--working-directory", default="/tmp")
    parser.add_argument("-e", "--environment", default="production-cloud")
    parser.add_argument("-u", "--upload-bucket", default="gitential-internal-data")
    args = parser.parse_args()
    print(args)

    prefix = f"{args.working_directory}/{args.date}_{args.environment}_usage_stats"

    usage_stat_json_file = f"{prefix}.json"
    if get_usage_stats(usage_stat_json_file, args.environment):
        # We've got usage stats, generate the csv and xls files
        usage_stats_csv_file_names = create_usage_stat_csvs_and_xls(prefix)

        # Upload to AWS S3
        file_names_to_upload = [usage_stat_json_file, *usage_stats_csv_file_names]
        _log(file_names_to_upload)
        upload_files(file_names_to_upload, args.upload_bucket, args.environment)
