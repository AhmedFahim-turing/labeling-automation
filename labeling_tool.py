from urllib.parse import quote
import requests
import pandas as pd
import asyncio
import argparse
from utils.constants import PROJECT_IDS
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
    key, appscript_url = appscript_url.split("$$")

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

    review_df = fix_discardability(review_df)

    author_df = author_df[author_df.columns[author_df.notnull().sum() != 0]]
    review_df = review_df[review_df.columns[review_df.notnull().sum() != 0]]

    author_summary_share = make_author_share_df(author_df, review_df)

    reviewer_summary_share = make_reviewer_share_df(review_df)

    overall_stats_share = make_overall_stats(author_df, review_df)

    author_metrics_share = make_author_metrics_share_df(author_df, review_df)

    second_reviewer_summary_share = make_second_reviewer_share(review_df)

    requests.post(
        appscript_url,
        json={
            "projectID": project_id,
            "status": "pass",
            "Author_summary": make_share_json(author_summary_share),
            "First_Reviewer_summary": make_share_json(reviewer_summary_share),
            "Second_Reviewer_summary": make_share_json(second_reviewer_summary_share),
            "Overall_summary": make_share_json(overall_stats_share),
            "author_score": make_share_json(author_metrics_share),
            "Task_Status": make_share_json(
                pd.crosstab(task_df["Subject"], task_df["Status"]).reset_index()
            ),
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("bearer_token", type=str)
    parser.add_argument("appscript_url", type=str)
    args = parser.parse_args()

    for project_id in PROJECT_IDS:
        main(
            project_id=project_id,
            bearer_token=args.bearer_token,
            appscript_url=args.appscript_url,
        )
