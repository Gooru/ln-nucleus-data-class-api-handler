package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

@Table("collection_performance")
public class AJEntityCollectionPerformance extends Model {

    public static final String SELECT_DISTINCT_USERID_FOR_COURSE_ID = "SELECT DISTINCT(actor_id) FROM collection_performance WHERE class_id = ? AND course_id = ?";

    public static final String SELECT_ITEM_PERF_IN_CLASS =
        "SELECT timespent, score AS scoreInPercentage, max_score AS maxScore, reaction AS reaction, views AS attempts, is_graded, unit_id AS unitId, lesson_id  AS lessonId, collection_id AS collectionId, collection_type AS collectionType, session_id AS sessionId, updated_at, status, path_id AS pathId, path_type AS pathType FROM collection_performance WHERE class_id = ? AND course_id = ? AND actor_id = ? AND date_in_time_zone <= ? ORDER BY updated_at DESC offset ? limit ?";

    public static final String SELECT_IL_DISTINCT_USERID_FOR_COURSE_ID = "SELECT DISTINCT(actor_id) FROM collection_performance WHERE class_id is NULL AND course_id = ?";

    public static final String SELECT_IL_ITEM_PERF_IN_CLASS =
        "SELECT timespent, score AS scoreInPercentage, max_score AS maxScore, reaction AS reaction, views AS attempts, is_graded, unit_id AS unitId, lesson_id  AS lessonId, collection_id AS collectionId, collection_type AS collectionType, session_id AS sessionId, updated_at, status, path_id AS pathId, path_type AS pathType FROM collection_performance WHERE class_id IS NULL AND course_id = ? AND actor_id = ? AND date_in_time_zone <= ? ORDER BY updated_at DESC offset ? limit ?";

}
