from urllib.parse import quote
import requests
import pandas as pd
import asyncio
from cryptography.fernet import Fernet
from io import StringIO
import argparse
from utils.constants import PROJECT_IDS_2
from utils.utils import (
    get_tabs_urls,
    get_responses,
    make_share_json,
    parse_responses,
    prepare_task_df,
    make_author_df,
    make_author_share_df,
    make_review_df,
    make_reviewer_share_df,
    make_overall_stats,
    make_author_metrics_share_df,
    make_second_reviewer_share,
    fix_discardability,
)


def main(project_id: str, bearer_token: str, appscript_url: str):
    key, appscript_url = appscript_url.split("@@")

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

    review_df = make_review_df(review_dict, task_df["TaskID"])
    author_df = make_author_df(author_dict, task_df["TaskID"])

    if project_id == "449":
        f = Fernet(key.encode("utf-8"))
        with open("449_sub_task.csv.enc", "rb") as file:
            string_io = StringIO(f.decrypt(file.read()).decode("utf-8"))
            sub_task_df = pd.read_csv(string_io)
        with open("449_sub_author.csv.enc", "rb") as file:
            string_io = StringIO(f.decrypt(file.read()).decode("utf-8"))
            sub_author_df = pd.read_csv(
                string_io, parse_dates=["VersionCreatedDate", "VersionUpdatedDate"]
            )
            sub_author_df["VersionCreatedDate"] = sub_author_df[
                "VersionCreatedDate"
            ].dt.date
            sub_author_df["VersionUpdatedDate"] = sub_author_df[
                "VersionUpdatedDate"
            ].dt.date
        with open("449_sub_review.csv.enc", "rb") as file:
            string_io = StringIO(f.decrypt(file.read()).decode("utf-8"))
            sub_review_df = pd.read_csv(string_io, parse_dates=["SubmittedDate"])
            sub_review_df["SubmittedDate"] = sub_review_df["SubmittedDate"].dt.date

        task_df = pd.concat([task_df, sub_task_df], ignore_index=True)
        author_df = pd.concat([author_df, sub_author_df], ignore_index=True)
        review_df = pd.concat([review_df, sub_review_df], ignore_index=True)
    # author_df.to_csv(f"{project_id}_author.csv", index=False)
    # review_df.to_csv(f"{project_id}_review.csv", index=False)

    review_df = fix_discardability(review_df)

    author_df = author_df[author_df.columns[author_df.notnull().sum() != 0]]
    review_df = review_df[review_df.columns[review_df.notnull().sum() != 0]]

    author_summary_share = make_author_share_df(author_df, review_df)

    reviewer_summary_share = make_reviewer_share_df(review_df)

    overall_stats_share = make_overall_stats(author_df, review_df)

    author_metrics_share = make_author_metrics_share_df(author_df, review_df)

    second_reviewer_summary_share = make_second_reviewer_share(review_df)

    task_df = task_df.rename(
        columns={
            "rc_form_response_isQuestionCorrect": "Is_Question_Correct",
            "rc_form_response_hasImageInQuestionChoices": "Image Correctness",
        }
    )

    task_df["Image Correctness"] = task_df["Image Correctness"].replace(
        {
            "No": "Image not required or present correctly",
            "Image not Required": "Image not required or present correctly",
            "Image Required and is provided and correct": "Image not required or present correctly",
            "Yes": "Image required but is absent or incorrect",
            "Image Required but not provided or is Incorrect": "Image required but is absent or incorrect",
        }
    )
    sub_task_df = task_df[task_df["Status"].str.contains("Done")]

    requests.post(
        appscript_url,
        json={
            "projectID": project_id,
            "status": "pass",
            "Author_summary": make_share_json(
                author_summary_share.drop(columns=["Num_0_or_1_Correctness"])
            ),
            "First_Reviewer_summary": make_share_json(reviewer_summary_share),
            "Second_Reviewer_summary": make_share_json(second_reviewer_summary_share),
            "Overall_summary": make_share_json(
                overall_stats_share.drop(
                    columns=[
                        "Num_0_or_1_Correctness_made",
                        "Num_0_or_1_Correctness_First_Done",
                        "Num_0_or_1_Correctness_Second_Done",
                    ]
                )
            ),
            "author_score": make_share_json(
                author_metrics_share.drop(columns=["Num_0_or_1_Correctness"])
            ),
            "Task_Status": make_share_json(
                pd.crosstab(task_df["Subject"], task_df["Status"]).reset_index()
            ),
            "Task_Status_2": make_share_json(
                pd.crosstab(
                    [sub_task_df["Subject"], sub_task_df["Is_Question_Correct"]],
                    sub_task_df["Image Correctness"],
                ).reset_index()
            ),
        },
    )


# main()
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("bearer_token", type=str)
    parser.add_argument("appscript_url", type=str)
    args = parser.parse_args()

    for project_id in PROJECT_IDS_2:
        main(
            project_id=project_id,
            bearer_token=args.bearer_token,
            appscript_url=args.appscript_url,
        )
