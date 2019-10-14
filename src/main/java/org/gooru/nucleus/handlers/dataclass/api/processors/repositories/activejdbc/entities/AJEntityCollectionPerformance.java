package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

@Table("collection_performance")
public class AJEntityCollectionPerformance extends Model {

  private static final String CLASS_ID = "course_id";
  private static final String COURSE_ID = "course_id";
  private static final String UNIT_ID = "unit_id";
  private static final String LESSON_ID = "lesson_id";
  private static final String COLLECTION_ID = "collection_id";
  private static final String SESSION_ID = "session_id";
  private static final String COLLECTION_TYPE = "collection_type";
  private static final String PATH_ID = "path_id";
  private static final String PATH_TYPE = "path_type";
  public static final String TIMESPENT = "timespent";
  public static final String MAX_SCORE = "max_score";
  public static final String SCORE = "score";
  public static final String REACTION = "reaction";
  public static final String VIEWS = "views";
  public static final String IS_GRADED = "is_graded";
  public static final String UPDATED_AT = "updated_at";
  public static final String STATUS = "status";
  public static final String CONTENT_SOURCE = "content_source";

  public static final String SELECT_DISTINCT_USERID_FOR_COURSE_ID =
      "SELECT DISTINCT(actor_id) FROM collection_performance WHERE class_id = ? AND course_id = ?";

  public static final String SELECT_ITEM_PERF_IN_CLASS =
      "SELECT timespent, score AS scoreInPercentage, max_score AS maxScore, reaction AS reaction, views AS attempts, is_graded, unit_id AS unitId, lesson_id  AS lessonId, collection_id AS collectionId, collection_type AS collectionType, session_id AS sessionId, updated_at, status, path_id AS pathId, path_type AS pathType, content_source AS contentSource FROM collection_performance WHERE class_id = ? AND course_id = ? AND actor_id = ? AND date_in_time_zone <= ? ORDER BY updated_at DESC offset ? limit ?";

  public static final String SELECT_IL_DISTINCT_USERID_FOR_COURSE_ID =
      "SELECT DISTINCT(actor_id) FROM collection_performance WHERE class_id is NULL AND course_id = ?";

  public static final String SELECT_IL_ITEM_PERF_IN_CLASS =
      "SELECT timespent, score AS scoreInPercentage, max_score AS maxScore, reaction AS reaction, views AS attempts, is_graded, unit_id AS unitId, lesson_id  AS lessonId, collection_id AS collectionId, collection_type AS collectionType, session_id AS sessionId, updated_at, status, path_id AS pathId, path_type AS pathType, content_source AS contentSource FROM collection_performance WHERE class_id IS NULL AND course_id = ? AND actor_id = ? AND date_in_time_zone <= ? ORDER BY updated_at DESC offset ? limit ?";

  public static final String GET_STUDENT_CLASS_ACTIVITY_START_DATE =
      "SELECT date_in_time_zone from collection_performance where "
          + " class_id = ? AND course_id = ? AND actor_id = ? ORDER BY date_in_time_zone ASC LIMIT 1";

  public static final String GET_IL_ACTIVITY_START_DATE =
      "SELECT date_in_time_zone from collection_performance where "
          + " course_id = ? AND actor_id = ? ORDER BY date_in_time_zone ASC LIMIT 1";

  public static final String FETCH_SUGG_ITEM_PERFORMANCE_IN_CLASS =
      "SELECT timespent, score, max_score, reaction, views, is_graded, class_id, course_id, unit_id, lesson_id,"
          + " collection_id, collection_type, session_id, updated_at, status, path_id, path_type, content_source "
          + " FROM collection_performance WHERE class_id = ? AND actor_id = ? "
          + " AND path_id = ANY(?::bigint[]) AND path_id > 0 AND content_source = ? ORDER BY updated_at DESC";

  public static final String FETCH_ALL_SUGG_ITEM_PERFORMANCE =
      "SELECT timespent, score, max_score, reaction, views, is_graded, class_id, course_id, unit_id, lesson_id,"
          + " collection_id, collection_type, session_id, updated_at, status, path_id, path_type, content_source "
          + " FROM collection_performance WHERE actor_id = ? "
          + " AND path_id = ANY(?::bigint[]) AND path_id > 0 AND content_source = ? ORDER BY updated_at DESC";

  public static final String SELECT_DISTINCT_USERID_FOR_CLASS_SUGGESTIONS =
      "SELECT DISTINCT(actor_id) FROM collection_performance WHERE class_id = ? AND "
          + " path_id = ANY(?::bigint[]) AND path_id > 0 AND content_source = ? ";

  public static final String SELECT_DISTINCT_USERID_FOR_SUGGESTIONS =
      "SELECT DISTINCT(actor_id) FROM collection_performance WHERE "
          + " path_id = ANY(?::bigint[]) AND path_id > 0 AND content_source = ? ";

  public static Boolean isValidScoreForCollection(Double score, Double maxScore) {
    return ((maxScore != null && maxScore > 0) && score != null);
  }

  public String getClassId() {
    return this.getString(CLASS_ID);
  }

  public String getCourseId() {
    return this.getString(COURSE_ID);
  }

  public String getUnitId() {
    return this.getString(UNIT_ID);
  }

  public String getLessonId() {
    return this.getString(LESSON_ID);
  }

  public String getCollectionId() {
    return this.getString(COLLECTION_ID);
  }

  public String getSessionId() {
    return this.getString(SESSION_ID);
  }

  public Long getPathId() {
    return this.getLong(PATH_ID);
  }

  public String getPathType() {
    return this.getString(PATH_TYPE);
  }

  public String getContentSource() {
    return this.getString(CONTENT_SOURCE);
  }

  public Long getTimespent() {
    return this.getLong(TIMESPENT);
  }

  public Double getScore() {
    return this.getDouble(SCORE);
  }

  public Double getMaxScore() {
    return this.getDouble(MAX_SCORE);
  }

  public Integer getReaction() {
    return this.getInteger(REACTION);
  }

  public String getCollectionType() {
    return this.getString(COLLECTION_TYPE);
  }

  public Boolean getIsGraded() {
    return this.getBoolean(IS_GRADED);
  }

  public Integer getViews() {
    return this.getInteger(VIEWS);
  }

  public String getUpdatedAt() {
    return this.getString(UPDATED_AT);
  }

  public Boolean getStatus() {
    return this.getBoolean(STATUS);
  }
}
