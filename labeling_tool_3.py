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
    make_reviewer_agg,
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

    review_df = make_review_df(review_dict, task_df["TaskID"], convert_to_date=True)
    author_df = make_author_df(author_dict, task_df["TaskID"], convert_to_date=True)

    author_df["form_stage"] = author_df.apply(
        lambda x: (
            x["form_stage"] if x["VersionNumber"] > 0 else "stage1 - Question Design"
        ),
        axis=1,
    )
    # author_df.to_csv(f"{project_id}_author.csv", index=False)
    # review_df.to_csv(f"{project_id}_review.csv", index=False)

    # author_df["VersionUpdatedDate"] = author_df["VersionUpdatedDate"].dt.tz_convert(
    #     None
    # )
    author_df["form_stage_short"] = (
        author_df["form_stage"].str.split("-").apply(lambda x: x[0].strip())
    )
    author_df["combined_status"] = (
        author_df["form_stage_short"] + "-" + author_df["Rework_or_NewTask"]
    )
    author_df["combined_status"] = author_df["combined_status"].replace(
        {"stage2-Rework": "stage2"}
    )
    df_counts = pd.pivot_table(
        author_df,
        index=["Author", "VersionCreatedDate"],
        values="TaskID",
        aggfunc="count",
        columns="combined_status",
    )
    df_mean = pd.pivot_table(
        author_df,
        index=["Author", "VersionCreatedDate"],
        values="durationMinutes",
        aggfunc="mean",
        columns="combined_status",
    )
    df_author_final = df_counts.join(
        df_mean,
        how="outer",
        lsuffix="_count",
        rsuffix="_avg_duration",
        validate="one_to_one",
    ).reset_index()
    review_agg = make_reviewer_agg(review_df)

    inprogress_task = task_df[
        (
            (task_df["task_status"] == "completed")
            & (task_df["formStage"] == "stage1 - Question Design")
        )
        # | (
        #     (task_df["task_status"] != "completed")
        #     & (task_df["formStage"] == "stage2 - Evaluating Model Pass@4")
        # )
        | (task_df["tab"] == "inprogress")
    ]
    rework_task = task_df[task_df["task_status"] == "rework"]
    delivery_task = task_df[task_df["tab"] == "delivery"]
    review_task_time_dict = (
        review_df.groupby("TaskID")["durationMinutes"].sum().to_dict()
    )
    # print(task_df[task_df["Subject"] == "biology"]["Status"].value_counts())
    # print(author_df.columns)
    reviewed_task = task_df[task_df["Status"].str.contains("Review Done")].reset_index(
        drop=True
    )
    reviewed_task["review_duration"] = reviewed_task["TaskID"].map(
        review_task_time_dict
    )
    # reviewed_task = reviewed_task.merge(
    #     task_df[["TaskID", "Subject"]], on="TaskID", validate="many_to_one"
    # )
    # print(reviewed_task.columns)
    completed_task = task_df[
        (task_df["task_status"] == "completed")
        & (task_df["formStage"] == "stage2 - Evaluating Model Pass@4")
    ]
    completed_agg = (
        reviewed_task.merge(
            author_df[
                [
                    "TaskID",
                    "durationMinutes",
                    #    "ConversationVersionID"
                ]
            ],
            on="TaskID",
        )
        .groupby("Subject")
        .agg(
            {
                "TaskID": "nunique",
                "durationMinutes": "sum",
                # "ConversationVersionID": "nunique",
            }
        )
        .rename(
            columns={
                "TaskID": "Num Tasks Reviewed",
                "durationMinutes": "Author time on tasks",
                # "review_duration": "Reviewer time on tasks",
            }
        )
        .reset_index()
        .merge(
            reviewed_task.groupby("Subject")[["review_duration"]]
            .sum()
            .reset_index()
            .rename(columns={"review_duration": "Reviewer time on tasks"}),
            on="Subject",
            how="outer",
        )
        .merge(
            completed_task["Subject"]
            .value_counts()
            .reset_index()
            .rename(columns={"count": "Num Tasks Completed by Author"}),
            on="Subject",
            how="outer",
        )
        .merge(
            task_df["Subject"]
            .value_counts()
            .reset_index()
            .rename(columns={"count": "Num Total Tasks"}),
            on="Subject",
            how="outer",
        )
        .merge(
            inprogress_task["Subject"]
            .value_counts()
            .reset_index()
            .rename(columns={"count": "Num Tasks In Progress"}),
            on="Subject",
            how="outer",
        )
        .merge(
            rework_task["Subject"]
            .value_counts()
            .reset_index()
            .rename(columns={"count": "Num Tasks In Rework"}),
            on="Subject",
            how="outer",
        )
        .merge(
            delivery_task["Subject"]
            .value_counts()
            .reset_index()
            .rename(columns={"count": "Num Tasks Delivered"}),
            on="Subject",
            how="outer",
        )
        .fillna(
            {
                "Num Tasks In Progress": 0,
                "Num Tasks In Rework": 0,
                "Num Tasks Delivered": 0,
            }
        )
    )
    completed_agg["Num Tasks To Do"] = completed_agg["Num Total Tasks"] - (
        completed_agg["Num Tasks Completed by Author"]
        + completed_agg["Num Tasks In Progress"]
        + completed_agg["Num Tasks In Rework"]
    )
    completed_agg["Num Tasks Ready For Review"] = (
        completed_agg["Num Tasks Completed by Author"]
        - completed_agg["Num Tasks Reviewed"]
    )
    completed_agg["Total time on tasks"] = (
        completed_agg["Author time on tasks"] + completed_agg["Reviewer time on tasks"]
    )
    completed_agg["Average time per task"] = (
        completed_agg["Total time on tasks"] / completed_agg["Num Tasks Reviewed"]
    )
    author_df["VersionCreatedDate_dedup"] = (
        author_df["VersionCreatedDate"].astype(str) + "_" + author_df.index.astype(str)
    )
    completed_task_date = completed_task[["TaskID"]].merge(
        author_df.groupby("TaskID")["VersionCreatedDate_dedup"].max().reset_index(),
        on="TaskID",
    )
    completed_task_date["Completed"] = 1
    author_df = author_df.merge(
        completed_task_date, on=["TaskID", "VersionCreatedDate_dedup"], how="left"
    ).drop(columns=["VersionCreatedDate_dedup"])
    author_df["Completed"] = author_df["Completed"].fillna(0).astype(int)

    review_df["SubmittedDate_dedup"] = (
        review_df["SubmittedDate"].astype(str) + "_" + review_df.index.astype(str)
    )
    reviewed_task_date = reviewed_task[["TaskID"]].merge(
        review_df.groupby("TaskID")["SubmittedDate_dedup"].max().reset_index(),
        on="TaskID",
    )
    reviewed_task_date["Final_Reviewed"] = 1
    review_df = review_df.merge(
        reviewed_task_date, on=["TaskID", "SubmittedDate_dedup"], how="left"
    ).drop(columns=["SubmittedDate_dedup"])
    review_df["Final_Reviewed"] = review_df["Final_Reviewed"].fillna(0).astype(int)
    # review_df["SubmittedDate"] = review_df["SubmittedDate"].astype(str)
    # review_df["SubmittedDate_date"] = review_df['SubmittedDate']
    # print(author_df["VersionCreatedDate"].dtype, review_df["SubmittedDate"].dtype)

    date_agg = (
        author_df.groupby("VersionCreatedDate")
        .agg({"Completed": "sum", "Author": "nunique"})
        .reset_index()
        .rename(
            columns={
                "Completed": "Num completed by author",
                "Author": "Num Authors",
                "VersionCreatedDate": "Date",
            }
        )
        .merge(
            review_df.groupby("SubmittedDate")
            .agg({"Final_Reviewed": "sum", "Reviewer": "nunique"})
            .reset_index()
            .rename(
                columns={
                    "Final_Reviewed": "Num Reviewed",
                    "Reviewer": "Num Reviewers",
                    "SubmittedDate": "Date",
                }
            ),
            on="Date",
            how="outer",
            validate="one_to_one",
        )
    )
    # df_author_final.to_csv("df_author_final.csv", index=False)
    # review_agg.to_csv("review_agg.csv", index=False)
    # date_agg.to_csv("date_agg.csv", index=False)
    # completed_agg.to_csv("completed_agg.csv", index=False)

    requests.post(
        appscript_url,
        json={
            "projectID": project_id,
            "status": "pass",
            "AuthorSummary": make_share_json(df_author_final),
            "ReviewerSummary": make_share_json(review_agg),
            "DailySummary": make_share_json(date_agg),
            "SubjectSummary": make_share_json(completed_agg),
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
