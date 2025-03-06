from urllib.parse import quote
import requests
from collections import defaultdict
import pandas as pd
import numpy as np
import json
import asyncio
import argparse

project_ids = ["254", "366"]

onboarding_batch_map = {
    "254": set(
        [
            1073,
        ]
    ),
    "366": set(
        [
            1157,
        ]
    ),
}


async def http_get(url: str, bearer_token: str):
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    return await asyncio.to_thread(
        requests.get, quote(url, safe=":/=?&"), headers=headers
    )


async def get_responses(url_list: list[str], bearer_token: str):
    return await asyncio.gather(
        *[http_get(url, bearer_token=bearer_token) for url in url_list]
    )


def author_metric_group(input_df):
    new_samples = input_df[input_df["Rework_or_NewTask"] == "New_Task"]

    return pd.Series(
        {
            "Num_samples": len(input_df),
            "New_samples": len(new_samples),
            "Num_Reviewed": new_samples["score"].count(),
            "score": input_df["score"].mean(),
            "avg_duration_min": input_df["durationMinutes"].mean(),
            "Completeness": input_df["Completeness"].mean(),
            "Language_Quality": input_df["Language Quality"].mean(),
            "Question_Correctness": input_df["Question correctness"].mean(),
            "Answer_Correctness": input_df["Accuracy of the Final Answer"].mean(),
            "Image_Correctness": input_df["Image correctness"].mean(),
            "Gemini_link_correctness": input_df["Gemini link correctness"].mean(),
            "Answer_Format_Correctness": input_df["Answer Format Correctness"].mean(),
            "Num_0_or_1_Correctness": new_samples["Has_0_or_1_Correctness"].sum(),
            "Num_Reworks": new_samples["Rework_task"].sum(),
        }
    )


def make_share_json(df: pd.DataFrame):
    for col in df.columns:
        if col.lower().endswith("date"):
            df[col] = df[col].astype(str)
    return [df.columns.tolist()] + df.fillna("").astype(str).fillna("").values.tolist()


quality_dim_id_mapping = {
    1: "Completeness",
    2: "Language Quality",
    29: "Accuracy of the Final Answer",
    31: "Question correctness",
    32: "Image correctness",
    33: "Gemini link correctness",
    35: "Answer Format Correctness",
}

rework_check_0 = "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[5]=statement||$cont||numberOfCorrectLinks**_-_0&filter[1]=status||$eq||rework&filter[2]=batch.status||$ne||draft&filter[3]=projectId||$eq||{project_id}&filter[4]=batch.status||$ne||draft&join[0]=project||id&join[1]=batch||id,status,projectId"
rework_check_1 = "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[5]=statement||$cont||numberOfCorrectLinks**_-_1&filter[1]=status||$eq||rework&filter[2]=batch.status||$ne||draft&filter[3]=projectId||$eq||{project_id}&filter[4]=batch.status||$ne||draft&join[0]=project||id&join[1]=batch||id,status,projectId"

rework = "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=status||$eq||rework&filter[2]=batch.status||$ne||draft&filter[3]=projectId||$eq||{project_id}&filter[4]=batch.status||$ne||draft&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=versions||id,durationMinutes,createdAt,updatedAt,author&join[3]=versions.author||id,turingEmail&join[4]=reviews||id,submittedAt,status,score,conversationVersionId,durationMinutes&join[5]=reviews.qualityDimensionValues||id,score,qualityDimensionId&join[6]=reviews.reviewer||id,turingEmail"

pending_check_0 = "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=status||$eq||completed&filter[2]=$needFollowup||$eq||true&filter[3]=projectId||$eq||{project_id}&filter[4]=batch.status||$ne||draft&filter[5]=statement||$cont||numberOfCorrectLinks**_-_0&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=latestManualReview&join[3]=latestManualReview.review||id&join[4]=statusHistory||id"
pending_check_1 = "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=status||$eq||completed&filter[2]=$needFollowup||$eq||true&filter[3]=projectId||$eq||{project_id}&filter[4]=batch.status||$ne||draft&filter[5]=statement||$cont||numberOfCorrectLinks**_-_1&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=latestManualReview&join[3]=latestManualReview.review||id&join[4]=statusHistory||id"

pending = "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=status||$eq||completed&filter[2]=$needFollowup||$eq||true&filter[3]=projectId||$eq||{project_id}&filter[4]=batch.status||$ne||draft&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=latestManualReview&join[3]=latestManualReview.review||id&join[4]=statusHistory||id&join[5]=versions||id,durationMinutes,createdAt,updatedAt,author&join[6]=versions.author||id,turingEmail&join[7]=reviews||id,submittedAt,status,score,conversationId,reviewerId,conversationVersionId,durationMinutes,qualityDimensionValues&join[8]=reviews.qualityDimensionValues||id,score,qualityDimensionId&join[9]=reviews.reviewer||id,turingEmail"

reviewed_check_0 = "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=latestDeliveryBatch.deliveryBatch||$isnull&filter[2]=$isReviewed||$eq||true&filter[3]=status||$eq||completed&filter[4]=batch.status||$ne||draft&filter[5]=manualReview.followupRequired||$eq||false&filter[6]=projectId||$eq||{project_id}&filter[7]=batch.status||$ne||draft&filter[8]=statement||$cont||numberOfCorrectLinks**_-_0&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=latestManualReview&join[3]=latestManualReview.review||id&join[4]=reviews||id&join[5]=latestDeliveryBatch"
reviewed_check_1 = "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=latestDeliveryBatch.deliveryBatch||$isnull&filter[2]=$isReviewed||$eq||true&filter[3]=status||$eq||completed&filter[4]=batch.status||$ne||draft&filter[5]=manualReview.followupRequired||$eq||false&filter[6]=projectId||$eq||{project_id}&filter[7]=batch.status||$ne||draft&filter[8]=statement||$cont||numberOfCorrectLinks**_-_1&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=latestManualReview&join[3]=latestManualReview.review||id&join[4]=reviews||id&join[5]=latestDeliveryBatch"

reviewed = "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=latestDeliveryBatch.deliveryBatch||$isnull&filter[2]=$isReviewed||$eq||true&filter[3]=status||$eq||completed&filter[4]=batch.status||$ne||draft&filter[5]=manualReview.followupRequired||$eq||false&filter[6]=projectId||$eq||{project_id}&filter[7]=batch.status||$ne||draft&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=latestManualReview&join[3]=latestManualReview.review||id&join[4]=reviews||id,submittedAt,status,score,conversationId,reviewerId,conversationVersionId,durationMinutes&join[5]=reviews.qualityDimensionValues||id,score,qualityDimensionId&join[6]=reviews.reviewer||id,turingEmail&join[7]=latestDeliveryBatch&join[8]=versions||id,durationMinutes,createdAt,updatedAt,author&join[9]=versions.author||id,turingEmail"

delivery_check_0 = "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=latestDeliveryBatch.deliveryBatch||$notnull&filter[2]=$isReviewed||$eq||true&filter[3]=status||$eq||completed&filter[4]=batch.status||$ne||draft&filter[5]=manualReview.followupRequired||$eq||false&filter[6]=projectId||$eq||{project_id}&filter[7]=batch.status||$ne||draftdraft&filter[8]=statement||$cont||numberOfCorrectLinks**_-_0&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=latestManualReview&join[3]=latestManualReview.review||id&join[4]=reviews||id&join[5]=latestDeliveryBatch"
delivery_check_1 = "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=latestDeliveryBatch.deliveryBatch||$notnull&filter[2]=$isReviewed||$eq||true&filter[3]=status||$eq||completed&filter[4]=batch.status||$ne||draft&filter[5]=manualReview.followupRequired||$eq||false&filter[6]=projectId||$eq||{project_id}&filter[7]=batch.status||$ne||draftdraft&filter[8]=statement||$cont||numberOfCorrectLinks**_-_1&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=latestManualReview&join[3]=latestManualReview.review||id&join[4]=reviews||id&join[5]=latestDeliveryBatch"

delivery = "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=latestDeliveryBatch.deliveryBatch||$notnull&filter[2]=$isReviewed||$eq||true&filter[3]=status||$eq||completed&filter[4]=batch.status||$ne||draft&filter[5]=manualReview.followupRequired||$eq||false&filter[6]=projectId||$eq||{project_id}&filter[7]=batch.status||$ne||draft&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=latestManualReview&join[3]=latestManualReview.review||id&join[4]=reviews||id,submittedAt,status,score,conversationId,reviewerId,conversationVersionId,durationMinutes&join[5]=reviews.qualityDimensionValues||id,score,qualityDimensionId&join[6]=reviews.reviewer||id,name,turingEmail&join[7]=latestDeliveryBatch&join[8]=versions||id,durationMinutes,createdAt,updatedAt,author&join[9]=versions.author||id,turingEmail"

zero_one_urls = [
    rework_check_0,
    rework_check_1,
    pending_check_0,
    pending_check_1,
    reviewed_check_0,
    reviewed_check_1,
    delivery_check_0,
    delivery_check_1,
]

main_urls = [rework, pending, reviewed, delivery]


def main(project_id: str, bearer_token: str, appscript_url: str):

    zero_one_urls_filled = [x.format(project_id=project_id) for x in zero_one_urls]
    main_urls_filled = [x.format(project_id=project_id) for x in main_urls]

    all_responses = asyncio.run(
        get_responses(
            main_urls_filled + zero_one_urls_filled, bearer_token=bearer_token
        )
    )

    try:
        for i in all_responses:
            assert i.status_code == 200, "Wrong Status Code"
    except:
        requests.post(appscript_url, json={"projectID": project_id, "status": "fail"})
        return

    task_list_zero_one = []

    for list_tasks in all_responses[-8:]:
        for task in list_tasks.json():
            task_list_zero_one.append(task["id"])

    author_dict = defaultdict(list)
    review_dict = defaultdict(list)

    q_dim_set = set(quality_dim_id_mapping.keys())

    for i, list_tasks in enumerate(all_responses[:-8]):
        for task in list_tasks.json():
            if task["batchId"] in onboarding_batch_map[project_id]:
                continue
            versions = task["versions"]
            reviews = [
                review for review in task["reviews"] if review["status"] == "published"
            ]
            for j, version in enumerate(versions):
                author_dict["TaskID"].append(task["id"])
                author_dict["ConversationVersionID"].append(version["id"])
                author_dict["Author"].append(version["author"]["turingEmail"])
                author_dict["VersionCreatedDate"].append(version["createdAt"])
                author_dict["VersionUpdatedDate"].append(version["updatedAt"])
                author_dict["durationMinutes"].append(version["durationMinutes"])
                if (j == 0) and (len(versions) > 1):
                    author_dict["Rework_task"].append(1)
                else:
                    author_dict["Rework_task"].append(np.nan)
                author_dict["VersionNumber"].append(j)
                if task["id"] in task_list_zero_one:
                    author_dict["Has_0_or_1_Correctness"].append("Yes")
                else:
                    author_dict["Has_0_or_1_Correctness"].append("No")
                if i > 1:
                    author_dict["Reviewed"].append("Yes")
                else:
                    author_dict["Reviewed"].append("No")
                if i == 3:
                    author_dict["Delivered"].append("Yes")
                else:
                    author_dict["Delivered"].append("No")

            for review in reviews:
                review_dict["TaskID"].append(task["id"])
                review_dict["ConversationVersionID"].append(
                    review["conversationVersionId"]
                )
                review_dict["Reviewer"].append(review["reviewer"]["turingEmail"])
                review_dict["SubmittedDate"].append(review["submittedAt"])
                review_dict["durationMinutes"].append(review["durationMinutes"])
                review_dict["score"].append(review["score"])
                q_added = set()
                for q_dim in review["qualityDimensionValues"]:
                    q_added.add(q_dim["qualityDimensionId"])
                    review_dict[
                        quality_dim_id_mapping[q_dim["qualityDimensionId"]]
                    ].append(q_dim["score"])
                for extra_col in q_dim_set - q_added:
                    review_dict[quality_dim_id_mapping[extra_col]].append(None)
                if task["id"] in task_list_zero_one:
                    review_dict["Has_0_or_1_Correctness"].append("Yes")
                else:
                    review_dict["Has_0_or_1_Correctness"].append("No")
                if i > 1:
                    review_dict["Reviewed"].append("Yes")
                else:
                    review_dict["Reviewed"].append("No")
                if i == 3:
                    review_dict["Delivered"].append("Yes")
                else:
                    review_dict["Delivered"].append("No")
    author_df = pd.DataFrame(author_dict)
    author_df["VersionCreatedDate"] = pd.to_datetime(author_df["VersionCreatedDate"])
    author_df["VersionUpdatedDate"] = pd.to_datetime(author_df["VersionUpdatedDate"])

    author_df["VersionCreatedDate"] = author_df["VersionCreatedDate"].dt.date
    author_df["VersionUpdatedDate"] = author_df["VersionUpdatedDate"].dt.date

    author_df["Has_0_or_1_Correctness"] = author_df["Has_0_or_1_Correctness"].map(
        {"Yes": 1, "No": 0}
    )

    review_df = pd.DataFrame(review_dict)
    review_df["SubmittedDate"] = pd.to_datetime(review_df["SubmittedDate"])
    review_df["SubmittedDate"] = review_df["SubmittedDate"].dt.date

    author_df["Rework_or_NewTask"] = author_df["VersionNumber"].apply(
        lambda x: "Rework" if x > 0 else "New_Task"
    )
    review_df["Has_0_or_1_Correctness"] = review_df["Has_0_or_1_Correctness"].map(
        {"Yes": 1, "No": 0}
    )

    grouped_review = review_df.groupby(
        ["TaskID", "ConversationVersionID"], as_index=False
    )[
        [
            "score",
            "Completeness",
            "Language Quality",
            "Question correctness",
            "Accuracy of the Final Answer",
            "Image correctness",
            "Gemini link correctness",
            "Answer Format Correctness",
        ]
    ].mean()

    reviewer_grouped = review_df.groupby(
        ["TaskID", "ConversationVersionID"], as_index=False
    )[["Reviewer"]].agg(set)

    author_df["Reviewer_changes"] = author_df.merge(
        reviewer_grouped, on=["TaskID", "ConversationVersionID"], how="left"
    ).apply(
        lambda x: (
            int(x["Author"] in x["Reviewer"])
            if isinstance(x["Reviewer"], set)
            else np.nan
        ),
        axis=1,
    )
    author_summary_share = (
        author_df.merge(
            grouped_review, on=["TaskID", "ConversationVersionID"], how="left"
        )
        .groupby(["Author", "VersionUpdatedDate", "Rework_or_NewTask"], as_index=False)
        .agg(
            Num_samples=("TaskID", "size"),
            Num_Reviewed=("score", "size"),
            score=("score", "mean"),
            avg_duration_min=("durationMinutes", "mean"),
            Completeness=("Completeness", "mean"),
            Language_Quality=("Language Quality", "mean"),
            Question_Correctness=("Question correctness", "mean"),
            Answer_correctness=("Accuracy of the Final Answer", "mean"),
            Image_Correctness=("Image correctness", "mean"),
            Gemini_link_correctness=("Gemini link correctness", "mean"),
            Answer_Format_Correctness=("Answer Format Correctness", "mean"),
            Num_0_or_1_Correctness=("Has_0_or_1_Correctness", "sum"),
            Reviewer_changes=("Reviewer_changes", "sum"),
            Num_Reworks=("Rework_task", "sum"),
        )
    )

    author_summary_share["Rework_percent"] = (
        author_summary_share["Num_Reworks"] / author_summary_share["Num_Reviewed"]
    )

    review_df_final = review_df.merge(
        author_df[["TaskID", "ConversationVersionID", "Reviewer_changes"]],
        on=["TaskID", "ConversationVersionID"],
    )

    reviewer_summary_share = (
        review_df_final.groupby(
            ["Reviewer", "SubmittedDate", "Reviewed"], as_index=False
        )
        .agg(
            Num_samples=("TaskID", "size"),
            score=("score", "mean"),
            avg_duration_min=("durationMinutes", "mean"),
            Completeness=("Completeness", "mean"),
            Language_Quality=("Language Quality", "mean"),
            Question_Correctness=("Question correctness", "mean"),
            Answer_correctness=("Accuracy of the Final Answer", "mean"),
            Image_Correctness=("Image correctness", "mean"),
            Gemini_link_correctness=("Gemini link correctness", "mean"),
            Answer_Format_Correctness=("Answer Format Correctness", "mean"),
            Num_0_or_1_Correctness=("Has_0_or_1_Correctness", "sum"),
            Reviewer_changes=("Reviewer_changes", "sum"),
        )
        .rename(columns={"Reviewed": "Moved_to_Done"})
    )
    author_overall = (
        author_df[author_df["Rework_or_NewTask"] == "New_Task"]
        .groupby("VersionUpdatedDate", as_index=False)
        .agg(
            Num_new_samples=("TaskID", "size"),
            Num_0_or_1_Correctness_made=("Has_0_or_1_Correctness", "sum"),
        )
        .merge(
            author_df[author_df["Reviewer_changes"] != 1]
            .groupby("VersionUpdatedDate", as_index=False)
            .agg(
                Num_Active_Authors=("Author", "nunique"),
                Author_list=("Author", "unique"),
            ),
            how="outer",
            on="VersionUpdatedDate",
        )
        .rename({"VersionUpdatedDate": "Date"}, axis="columns")
    )

    reviewer_overall = (
        review_df[review_df["Reviewed"] == "Yes"]
        .groupby("SubmittedDate", as_index=False)
        .agg(
            Num_Moved_to_Done=("TaskID", "size"),
            Num_0_or_1_Correctness_Done=("Has_0_or_1_Correctness", "sum"),
        )
        .merge(
            review_df.groupby("SubmittedDate", as_index=False).agg(
                Num_Active_Reviewers=("Reviewer", "nunique"),
                Reviewer_list=("Reviewer", "unique"),
            ),
            how="outer",
        )
        .rename({"SubmittedDate": "Date"}, axis="columns")
    )

    overall_stats_share = author_overall.merge(
        reviewer_overall, how="outer", on="Date", validate="one_to_one"
    )
    overall_stats_share["Num_Active_Headcount"] = overall_stats_share.apply(
        lambda x: len(
            set(
                (
                    list(x["Author_list"])
                    if hasattr(x["Author_list"], "__iter__")
                    else []
                )
                + (
                    list(x["Reviewer_list"])
                    if hasattr(x["Reviewer_list"], "__iter__")
                    else []
                )
            )
        ),
        axis=1,
    )
    overall_stats_share = overall_stats_share.drop(
        columns=["Author_list", "Reviewer_list"]
    )

    author_metrics_share = (
        author_df.merge(
            grouped_review, on=["TaskID", "ConversationVersionID"], how="left"
        )
        .groupby(["Author", "VersionUpdatedDate"], as_index=False)
        .apply(author_metric_group, include_groups=False)
    )

    author_metrics_share["Rework_percent"] = (
        author_metrics_share["Num_Reworks"] / author_metrics_share["Num_Reviewed"]
    )
    requests.post(
        appscript_url,
        json={
            "projectID": project_id,
            "status": "pass",
            "Author_summary": make_share_json(author_summary_share),
            "Reviewer_summary": make_share_json(reviewer_summary_share),
            "Overall_summary": make_share_json(overall_stats_share),
            "author_score": make_share_json(author_metrics_share),
        },
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("bearer_token", type=str)
    parser.add_argument("appscript_url", type=str)
    args = parser.parse_args()

    for project_id in project_ids:
        main(
            project_id=project_id,
            bearer_token=args.bearer_token,
            appscript_url=args.appscript_url,
        )
