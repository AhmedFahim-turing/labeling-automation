from urllib.parse import quote
import requests
from datetime import datetime
import pandas as pd
import asyncio

import argparse
from utils.constants import PROJECT_IDS_4
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
    task_df["formStage"] = task_df["formStage"].str.strip()
    # task_df.to_csv(f"{project_id}_task.csv", index=False)

    # review_df = make_review_df(review_dict, task_df["TaskID"], convert_to_date=False)
    author_df = make_author_df(author_dict, task_df["TaskID"], convert_to_date=True)

    # author_df.to_csv(f"{project_id}_author.csv", index=False)
    # review_df.to_csv(f"{project_id}_review.csv", index=False)

    # author_df["VersionUpdatedDate"] = author_df["VersionUpdatedDate"].dt.tz_convert(
    #     None
    # )
    author_grouped = author_df.groupby("TaskID", as_index=False).apply(
        lambda x: x.sort_values("VersionUpdatedDate").iloc[-1], include_groups=False
    )[["TaskID"]]

    task_merged = task_df.merge(author_grouped, on="TaskID", how="left")
    task_merged["Task URL"] = task_merged["TaskID"].apply(
        lambda x: f"https://labeling-g.turing.com/conversations/{x}/view"
    )
    task_merged = task_merged[
        [
            "TaskID",
            "Task URL",
            "Subject",
            "task_status",
            "item_id",
            "batchName",
            "tab",
            "formStage",
            "Status",
            "Author",
        ]
    ]
    task_remaining = task_merged[
        ~(
            (task_merged["task_status"] == "completed")
            & (task_merged["formStage"] == "stage2 - Evaluating Model Pass@4")
        )
    ]
    task_completed = task_merged[
        (task_merged["task_status"] == "completed")
        & (task_merged["formStage"] == "stage2 - Evaluating Model Pass@4")
    ]
    requests.post(
        appscript_url,
        json={
            "projectID": project_id,
            "status": "pass",
            "StatusSheet": make_share_json(
                task_remaining.sort_values(["Author", "batchName"])
            ),
            "CompletedSamples": make_share_json(
                task_completed.sort_values(["Author", "batchName"]).drop(
                    columns=["Author"]
                )
            ),
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("bearer_token", type=str)
    parser.add_argument("appscript_url", type=str)
    args = parser.parse_args()

    for project_id in PROJECT_IDS_4:
        main(
            project_id=project_id,
            bearer_token=args.bearer_token,
            appscript_url=args.appscript_url,
        )
