import asyncio
from urllib.parse import quote
from collections import defaultdict
import requests
import numpy as np
import pandas as pd
from .constants import (
    REWORK,
    PENDING_REVIEW,
    REVIEWED,
    DELIVERY,
    ONBOARDING_BATCH_MAP,
    QUALITY_DIM_ID_MAPPING,
)


def get_tabs_urls(project_id: str):
    tabs, urls = zip(*[REWORK, PENDING_REVIEW, REVIEWED, DELIVERY])
    final_urls = [x.format(project_id=project_id) for x in urls]
    return tabs, final_urls


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


def get_subject_mapping_func(project_id):
    if project_id == "254":
        return lambda x: x.get(
            "rc_form_response_datasetDomainAndTopic", "::Not Found"
        ).split("::")[1]
    elif project_id == "366":
        return lambda x: x.get("rc_form_response_subjectAndUnit", "Not Found::").split(
            "::"
        )[0]
    else:
        raise Exception


def parse_responses(responses, tabs, project_id):
    task_dict = defaultdict(list)
    author_dict = defaultdict(list)
    review_dict = defaultdict(list)

    subject_mapping_func = get_subject_mapping_func(project_id)

    q_dim_set = set(QUALITY_DIM_ID_MAPPING.keys())

    for i, list_tasks in enumerate(responses):
        for task in list_tasks.json():
            if task["batchId"] in ONBOARDING_BATCH_MAP[project_id]:
                continue
            metadata_dict = {
                i[2 : i.rfind("**")]: i[i.rfind("** - ") + 5 :]
                for i in task["statement"].split("\n\n")[:-1]
            }
            task_dict["TaskID"].append(task["id"])
            task_dict["Num_Gemini_Correct"].append(
                metadata_dict.get("rc_form_response_numberOfCorrectLinks", np.nan)
            )
            task_dict["Subject"].append(subject_mapping_func(metadata_dict))
            task_dict["tab"].append(tabs[i])
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
            review_type = "First_Review"
            reworked = False
            num_positive_reviews = 0
            num_reviewed_tab = 0
            for review in reviews:
                review_dict["TaskID"].append(task["id"])
                review_dict["ConversationVersionID"].append(
                    review["conversationVersionId"]
                )
                review_dict["Reviewer"].append(review["reviewer"]["turingEmail"])
                review_dict["SubmittedDate"].append(review["submittedAt"])
                review_dict["durationMinutes"].append(review["durationMinutes"])
                review_dict["score"].append(review["score"])
                review_dict["stage"].append(review_type)
                review_dict["num_reviewed_tab"].append(num_reviewed_tab)
                q_added = set()
                for q_dim in review["qualityDimensionValues"]:
                    q_added.add(q_dim["qualityDimensionId"])
                    review_dict[
                        QUALITY_DIM_ID_MAPPING[q_dim["qualityDimensionId"]]
                    ].append(q_dim["score"])
                for extra_col in q_dim_set - q_added:
                    review_dict[QUALITY_DIM_ID_MAPPING[extra_col]].append(None)
                if review["followupRequired"]:
                    review_dict["Reviewed"].append("No")
                    reworked = True
                    num_reviewed_tab = 0
                else:
                    review_dict["Reviewed"].append("Yes")
                    review_type = "Second_Review"
                    reworked = False
                    num_positive_reviews += 1
                    num_reviewed_tab = 1
            task_dict["reworked"].append(reworked)
            task_dict["num_positive_reviews"].append(num_positive_reviews)

    return task_dict, author_dict, review_dict


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


def make_author_metrics_share_df(author_df, review_df):
    grouped_review = group_review(review_df)

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
    return author_metrics_share


def make_share_json(df: pd.DataFrame):
    for col in df.columns:
        if col.lower().endswith("date"):
            df[col] = df[col].astype(str)
    return [df.columns.tolist()] + df.fillna("").astype(str).fillna("").values.tolist()


def make_reviewer_agg(input_df) -> pd.DataFrame:
    first_df = (
        input_df.groupby(["Reviewer", "SubmittedDate", "Reviewed"])["TaskID"]
        .size()
        .reset_index()
    )
    first_df = (
        first_df.pivot(
            columns="Reviewed", index=["Reviewer", "SubmittedDate"], values="TaskID"
        )
        .reset_index()
        .rename(columns={"No": "Num_Reworks", "Yes": "Num_Done"})
    )

    second_df = (
        input_df.groupby(["Reviewer", "SubmittedDate", "Reviewed"])["durationMinutes"]
        .mean()
        .reset_index()
    )
    second_df = (
        second_df.pivot(
            columns="Reviewed",
            index=["Reviewer", "SubmittedDate"],
            values="durationMinutes",
        )
        .reset_index()
        .rename(columns={"No": "Rework_duration_min", "Yes": "Done_duration_min"})
    )

    return first_df.merge(
        second_df, on=["Reviewer", "SubmittedDate"], validate="one_to_one"
    )


def make_second_reviewer_share(review_df):
    second_df = review_df[review_df["stage"] == "Second_Review"]
    second_review_df = make_reviewer_agg(second_df)
    extra_info = (
        second_df[["Reviewer", "SubmittedDate", "num_reviewed_tab"]]
        .groupby(["Reviewer", "SubmittedDate"])["num_reviewed_tab"]
        .sum()
        .reset_index()
    )
    second_review_df = second_review_df.merge(
        extra_info, on=["Reviewer", "SubmittedDate"], how="left", validate="one_to_one"
    )
    return second_review_df


def make_reviewer_share_df(review_df: pd.DataFrame) -> pd.DataFrame:
    first_review_df = review_df[review_df["stage"] == "First_Review"]
    second_review_df = review_df[review_df["stage"] == "Second_Review"]

    first_review_agg = make_reviewer_agg(first_review_df)

    second_first_review = second_review_df.merge(
        first_review_df[
            ["TaskID", "ConversationVersionID", "Reviewer", "SubmittedDate"]
        ],
        on=["TaskID", "ConversationVersionID"],
        suffixes=("_second", ""),
    )

    third_df = (
        second_first_review.groupby(["Reviewer", "SubmittedDate"])["TaskID"]
        .nunique()
        .reset_index()
        .rename(columns={"TaskID": "Num_Second_Reviewed"})
    )
    fourth_df = (
        second_first_review[second_first_review["Reviewed"] == "No"]
        .groupby(["Reviewer", "SubmittedDate"])["TaskID"]
        .nunique()
        .reset_index()
        .rename(columns={"TaskID": "Num_Second_Rework"})
    )
    fifth_df = (
        second_first_review.groupby(["Reviewer", "SubmittedDate"])[
            [
                "score",
                "Completeness",
                "Language Quality",
                "Accuracy of the Final Answer",
                "Question correctness",
                "Image correctness",
                "Gemini link correctness",
                "Answer Format Correctness",
            ]
        ]
        .mean()
        .reset_index()
    )

    three_four_five = (
        third_df.merge(
            fourth_df,
            on=["Reviewer", "SubmittedDate"],
            how="left",
            validate="one_to_one",
        )
        .merge(
            fifth_df,
            on=["Reviewer", "SubmittedDate"],
            how="left",
            validate="one_to_one",
        )
        .fillna(value={"Num_Second_Rework": 0})
    )

    all_joined = first_review_agg.merge(
        three_four_five,
        on=["Reviewer", "SubmittedDate"],
        how="left",
        validate="one_to_one",
    )

    all_joined["Rework_percentage"] = (
        all_joined["Num_Second_Rework"] / all_joined["Num_Second_Reviewed"]
    )

    return all_joined


def assign_status(row):
    if row["tab"] == "delivery":
        return "Delivered"
    elif (
        (row["tab"] == "pending_review")
        and (row["num_positive_reviews"] == 0)
        and (not row["reworked"])
    ):
        return "Ready For Review"
    elif (row["tab"] == "rework") and (row["num_positive_reviews"] == 0):
        return "1st Review Comments Added"
    elif (
        (row["tab"] == "pending_review")
        and (row["num_positive_reviews"] == 0)
        and (row["reworked"])
    ):
        return "1st Review Comments Addressed"
    elif (row["tab"] == "reviewed") and (row["num_positive_reviews"] < 2):
        return "1st Review Done"
    elif (row["tab"] == "rework") and (row["num_positive_reviews"] > 0):
        return "2nd Review Comments Added"
    elif (
        (row["tab"] == "pending_review")
        and (row["num_positive_reviews"] > 0)
        and (row["reworked"])
    ):
        return "2nd Review Comments Addressed"
    elif (row["tab"] == "reviewed") and (row["num_positive_reviews"] > 1):
        return "2nd Review Done"
    return "Invalid Status"


def prepare_task_df(task_dict):
    task_df = pd.DataFrame(task_dict)
    task_df["Num_Gemini_Correct"] = pd.to_numeric(task_df["Num_Gemini_Correct"])

    zero_one_task_df = task_df[task_df["Num_Gemini_Correct"] < 2].reset_index(drop=True)
    zero_one_task_df["Status"] = zero_one_task_df.apply(assign_status, axis=1)
    return zero_one_task_df


def make_review_df(review_dict, tasks):
    review_df = pd.DataFrame(review_dict)
    review_df["SubmittedDate"] = pd.to_datetime(review_df["SubmittedDate"])
    review_df["SubmittedDate"] = review_df["SubmittedDate"].dt.date

    review_df["Has_0_or_1_Correctness"] = review_df["TaskID"].apply(
        lambda x: int(x in set(tasks))
    )
    return review_df


def make_author_df(author_dict, tasks):

    author_df = pd.DataFrame(author_dict)
    author_df["VersionCreatedDate"] = pd.to_datetime(author_df["VersionCreatedDate"])
    author_df["VersionUpdatedDate"] = pd.to_datetime(author_df["VersionUpdatedDate"])

    author_df["VersionCreatedDate"] = author_df["VersionCreatedDate"].dt.date
    author_df["VersionUpdatedDate"] = author_df["VersionUpdatedDate"].dt.date

    author_df["Has_0_or_1_Correctness"] = author_df.apply(
        lambda x: int(x["TaskID"] in set(tasks)) if x["VersionNumber"] == 0 else np.nan,
        axis=1,
    )

    author_df["Rework_or_NewTask"] = author_df["VersionNumber"].apply(
        lambda x: "Rework" if x > 0 else "New_Task"
    )
    return author_df


def group_review(review_df):
    return review_df.groupby(["TaskID", "ConversationVersionID"], as_index=False)[
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


def make_author_share_df(author_df, review_df):
    grouped_review = group_review(review_df)

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
            Num_Reviewed=("score", "count"),
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
    author_summary_share.loc[
        author_summary_share["Rework_or_NewTask"] == "Rework", "Num_0_or_1_Correctness"
    ] = np.nan
    return author_summary_share


def make_overall_stats(author_df, review_df):
    first_review_df = review_df[review_df["stage"] == "First_Review"]
    second_review_df = review_df[review_df["stage"] == "Second_Review"]

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

    first_reviewer_overall = (
        first_review_df[first_review_df["Reviewed"] == "Yes"]
        .groupby("SubmittedDate", as_index=False)
        .agg(
            Num_Moved_to_First_Done=("TaskID", "nunique"),
            Num_0_or_1_Correctness_First_Done=("Has_0_or_1_Correctness", "sum"),
        )
        .merge(
            first_review_df.groupby("SubmittedDate", as_index=False).agg(
                Num_Active_First_Reviewers=("Reviewer", "nunique"),
                First_Reviewer_list=("Reviewer", "unique"),
            ),
            how="outer",
        )
        .rename({"SubmittedDate": "Date"}, axis="columns")
    )
    second_reviewer_overall = (
        second_review_df[second_review_df["Reviewed"] == "Yes"]
        .groupby("SubmittedDate", as_index=False)
        .agg(
            Num_Moved_to_Second_Done=("TaskID", "nunique"),
            Num_0_or_1_Correctness_Second_Done=("Has_0_or_1_Correctness", "sum"),
        )
        .merge(
            second_review_df.groupby("SubmittedDate", as_index=False).agg(
                Num_Active_Second_Reviewers=("Reviewer", "nunique"),
                Second_Reviewer_list=("Reviewer", "unique"),
            ),
            how="outer",
        )
        .rename({"SubmittedDate": "Date"}, axis="columns")
    )

    overall_stats_share = author_overall.merge(
        first_reviewer_overall, how="outer", on="Date", validate="one_to_one"
    ).merge(second_reviewer_overall, how="outer", on="Date", validate="one_to_one")
    overall_stats_share["Num_Active_Headcount"] = overall_stats_share.apply(
        lambda x: len(
            set(
                (
                    list(x["Author_list"])
                    if hasattr(x["Author_list"], "__iter__")
                    else []
                )
                + (
                    list(x["First_Reviewer_list"])
                    if hasattr(x["First_Reviewer_list"], "__iter__")
                    else []
                )
                + (
                    list(x["Second_Reviewer_list"])
                    if hasattr(x["Second_Reviewer_list"], "__iter__")
                    else []
                )
            )
        ),
        axis=1,
    )
    overall_stats_share = overall_stats_share.drop(
        columns=["Author_list", "First_Reviewer_list", "Second_Reviewer_list"]
    )
    return overall_stats_share
