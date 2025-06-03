from urllib.parse import quote
import requests
from datetime import datetime
import pandas as pd
import asyncio

import argparse
from utils.constants import PROJECT_IDS_3
from utils.utils import (
    get_tabs_urls,
    get_responses,
    make_share_json,
    parse_responses,
    prepare_task_df,
    make_author_df,
    make_review_df,
)


def map_pending_review(series: pd.Series):
    if series["tab"] == "pending_review":
        if series["HasReviewer"]:
            return "pending_review_with_reviewer"
        else:
            return "pending_review_without_reviewer"
    return series["tab"]


def main(project_id: str, bearer_token: str, appscript_url: str):
    tabs, urls = get_tabs_urls(project_id)
    all_responses = asyncio.run(get_responses(urls, bearer_token=bearer_token))

    try:
        for i in all_responses:
            assert i.status_code == 200, "Wrong Status Code"
    except:
        requests.post(appscript_url, json={"projectID": project_id, "status": "fail"})
        return

    task_dict, author_dict, review_dict = parse_responses(
        all_responses, tabs, project_id
    )

    task_df = prepare_task_df(task_dict)
    # task_df.to_csv(f"{project_id}_task.csv", index=False)

    review_df = make_review_df(review_dict, task_df["TaskID"], convert_to_date=False)
    author_df = make_author_df(author_dict, task_df["TaskID"], convert_to_date=False)

    # author_df.to_csv(f"{project_id}_author.csv", index=False)
    # review_df.to_csv(f"{project_id}_review.csv", index=False)

    author_df["VersionUpdatedDate"] = author_df["VersionUpdatedDate"].dt.tz_convert(
        None
    )
    review_df["SubmittedDate"] = review_df["SubmittedDate"].dt.tz_convert(None)

    author_want = task_df[task_df["tab"] == "rework"].merge(
        author_df.sort_values("VersionUpdatedDate")
        .groupby("TaskID", as_index=False)[["Author", "VersionUpdatedDate"]]
        .last()[["TaskID", "Author", "VersionUpdatedDate"]],
        on="TaskID",
    )
    author_want["TaskLink"] = author_want["TaskID"].apply(
        lambda x: f"https://labeling-g.turing.com/conversations/{x}/view"
    )
    author_want["NumReworks"] = author_want.groupby("Author")["TaskID"].transform(
        "count"
    )
    version_updated_dict = (
        author_df.groupby("TaskID")["VersionUpdatedDate"].min().to_dict()
    )

    review_want = task_df[task_df["tab"] == "pending_review"].merge(
        review_df.sort_values("SubmittedDate")
        .groupby("TaskID", as_index=False)[["Reviewer", "SubmittedDate"]]
        .last()[["TaskID", "Reviewer", "SubmittedDate"]],
        on="TaskID",
    )
    review_want["NumReviews"] = review_want.groupby("Reviewer")["TaskID"].transform(
        "count"
    )
    review_want["TaskLink"] = review_want["TaskID"].apply(
        lambda x: f"https://labeling-g.turing.com/conversations/{x}/view"
    )
    review_submitted_dict = review_df.groupby("TaskID")["SubmittedDate"].min().to_dict()
    sub_df = task_df[
        task_df["tab"].isin(["rework", "pending_review", "unclaimed", "inprogress"])
    ].sort_values("batchId")
    sub_df["HasReviewer"] = sub_df["TaskID"].isin(review_df["TaskID"].unique())
    sub_df["tab"] = sub_df.apply(map_pending_review, axis=1)

    incomplete_batches = sub_df["batchName"].unique()
    complete_batches = task_df[~task_df["batchName"].isin(incomplete_batches)][
        ["batchName"]
    ].drop_duplicates(ignore_index=True)

    author_want["ReviewedDate"] = author_want["TaskID"].map(review_submitted_dict)
    author_want["DurationPending"] = (
        (datetime.now() - author_want["ReviewedDate"])
        .dt.total_seconds()
        .apply(lambda x: f"{int(x//3600)}:{int((x//60)%60)}:{int(x%60)}")
    )

    review_want["AuthorCompletedDate"] = review_want["TaskID"].map(version_updated_dict)
    review_want["DurationPending"] = (
        (datetime.now() - review_want["AuthorCompletedDate"])
        .dt.total_seconds()
        .apply(lambda x: f"{int(x//3600)}:{int((x//60)%60)}:{int(x%60)}")
    )

    requests.post(
        appscript_url,
        json={
            "projectID": project_id,
            "status": "pass",
            "Reworks": make_share_json(
                author_want[
                    [
                        "Author",
                        "batchName",
                        "TaskID",
                        "ReviewedDate",
                        "DurationPending",
                        "TaskLink",
                        "NumReworks",
                    ]
                ].sort_values(["Author", "ReviewedDate"])
            ),
            "Pending_Reviews": make_share_json(
                review_want[
                    [
                        "Reviewer",
                        "batchName",
                        "TaskID",
                        "AuthorCompletedDate",
                        "DurationPending",
                        "TaskLink",
                        "NumReviews",
                    ]
                ].sort_values(["Reviewer", "AuthorCompletedDate"])
            ),
            "Pending_Status": make_share_json(
                pd.crosstab(sub_df["batchName"], sub_df["tab"])
                .loc[sub_df["batchName"].unique()]
                .reset_index()
            ),
            "Complete_Batches": make_share_json(complete_batches),
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("bearer_token", type=str)
    parser.add_argument("appscript_url", type=str)
    args = parser.parse_args()

    for project_id in PROJECT_IDS_3:
        main(
            project_id=project_id,
            bearer_token=args.bearer_token,
            appscript_url=args.appscript_url,
        )
