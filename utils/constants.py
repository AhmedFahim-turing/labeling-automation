PROJECT_IDS = [
    # "254", "366",
    # "441",
    "472",
]
PROJECT_IDS_2 = [
    # "448",
    # "449",
    #  "471"
]
PROJECT_IDS_3 = ["441", "449"]

PROJECT_IDS_4 = ["547"]

ONBOARDING_BATCH_MAP = {
    "254": set([1073]),
    "366": set([1157]),
    "441": set([1379]),
}

QUALITY_DIM_ID_MAPPING = {
    1: "Completeness",
    2: "Language Quality",
    29: "Accuracy of the Final Answer",
    31: "Question correctness",
    32: "Image correctness",
    33: "Gemini link correctness",
    35: "Answer Format Correctness",
    24: "Correctness",
    171: "Gemini response correctness",
    172: "Question Discardability",
}

UNCLAIMED = (
    "unclaimed",
    "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=$isClaimed||$eq||false&filter[2]=status||$eq||pending&filter[3]=projectId||$eq||{project_id}&filter[4]=batch.status||$ne||draft&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=versions||id,durationMinutes,createdAt,updatedAt,author,formStage&join[3]=versions.author||id,turingEmail&join[4]=reviews||id,submittedAt,followupRequired,status,score,conversationVersionId,durationMinutes&join[5]=reviews.qualityDimensionValues||id,score,qualityDimensionId&join[6]=reviews.reviewer||id,turingEmail&join[7]=statusHistory||id,formStage",
)

INPROGRESS = (
    "inprogress",
    "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=$isClaimed||$eq||true&filter[2]=status||$eq||labeling&filter[3]=status||$in||labeling,validating&filter[4]=projectId||$eq||{project_id}&filter[5]=batch.status||$ne||draft&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=versions||id,durationMinutes,createdAt,updatedAt,author,formStage&join[3]=versions.author||id,turingEmail&join[4]=reviews||id,submittedAt,followupRequired,status,score,conversationVersionId,durationMinutes&join[5]=reviews.qualityDimensionValues||id,score,qualityDimensionId&join[6]=reviews.reviewer||id,turingEmail&join[7]=statusHistory||id,formStage",
)

REWORK = (
    "rework",
    "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=status||$eq||rework&filter[2]=batch.status||$ne||draft&filter[3]=projectId||$eq||{project_id}&filter[4]=batch.status||$ne||draft&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=versions||id,durationMinutes,createdAt,updatedAt,author,formStage&join[3]=versions.author||id,turingEmail&join[4]=reviews||id,submittedAt,followupRequired,status,score,conversationVersionId,durationMinutes&join[5]=reviews.qualityDimensionValues||id,score,qualityDimensionId&join[6]=reviews.reviewer||id,turingEmail&join[7]=statusHistory||id,formStage",
)

PENDING_REVIEW = (
    "pending_review",
    "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=status||$eq||completed&filter[2]=$needFollowup||$eq||true&filter[3]=projectId||$eq||{project_id}&filter[4]=batch.status||$ne||draft&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=latestManualReview&join[3]=latestManualReview.review||id&join[4]=statusHistory||id,formStage&join[5]=versions||id,durationMinutes,createdAt,updatedAt,author,formStage&join[6]=versions.author||id,turingEmail&join[7]=reviews||id,submittedAt,status,score,followupRequired,conversationId,reviewerId,conversationVersionId,durationMinutes,qualityDimensionValues&join[8]=reviews.qualityDimensionValues||id,score,qualityDimensionId&join[9]=reviews.reviewer||id,turingEmail",
)

REVIEWED = (
    "reviewed",
    "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=latestDeliveryBatch.deliveryBatch||$isnull&filter[2]=$isReviewed||$eq||true&filter[3]=status||$eq||completed&filter[4]=batch.status||$ne||draft&filter[5]=manualReview.followupRequired||$eq||false&filter[6]=projectId||$eq||{project_id}&filter[7]=batch.status||$ne||draft&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=latestManualReview&join[3]=latestManualReview.review||id&join[4]=reviews||id,submittedAt,status,score,followupRequired,conversationId,reviewerId,conversationVersionId,durationMinutes&join[5]=reviews.qualityDimensionValues||id,score,qualityDimensionId&join[6]=reviews.reviewer||id,turingEmail&join[7]=latestDeliveryBatch&join[8]=versions||id,durationMinutes,createdAt,updatedAt,author,formStage&join[9]=versions.author||id,turingEmail&join[10]=statusHistory||id,formStage",
)

DELIVERY = (
    "delivery",
    "https://labeling-g.turing.com/api/conversations/download-filtered-conversations/json?limit=10&page=1&filter[0]=batch.status||$ne||draft&filter[1]=latestDeliveryBatch.deliveryBatch||$notnull&filter[2]=$isReviewed||$eq||true&filter[3]=status||$eq||completed&filter[4]=batch.status||$ne||draft&filter[5]=manualReview.followupRequired||$eq||false&filter[6]=projectId||$eq||{project_id}&filter[7]=batch.status||$ne||draft&join[0]=project||id&join[1]=batch||id,status,projectId&join[2]=latestManualReview&join[3]=latestManualReview.review||id&join[4]=reviews||id,submittedAt,status,score,followupRequired,conversationId,reviewerId,conversationVersionId,durationMinutes&join[5]=reviews.qualityDimensionValues||id,score,qualityDimensionId&join[6]=reviews.reviewer||id,name,turingEmail&join[7]=latestDeliveryBatch&join[8]=versions||id,durationMinutes,createdAt,updatedAt,author,formStage&join[9]=versions.author||id,turingEmail&join[10]=latestDeliveryBatch.deliveryBatch||name&join[11]=statusHistory||id,formStage",
)
