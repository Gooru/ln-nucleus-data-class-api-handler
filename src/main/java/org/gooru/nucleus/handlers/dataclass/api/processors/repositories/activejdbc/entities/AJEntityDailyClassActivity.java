package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import java.util.regex.Pattern;
import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;

/**
 * Created by mukul@gooru
 * 
 */

@Table("daily_class_activity")
public class AJEntityDailyClassActivity extends Model {


  public static final String ID = "id";
  public static final String EVENTNAME = "event_name";

  public static final String EVENTTYPE = "event_type";
  // actor_id is userId or gooruuid
  public static final String GOORUUID = "actor_id";
  public static final String TENANT_ID = "tenant_id";

  public static final Object CLASS_GOORU_OID = "class_id";
  public static final String COURSE_GOORU_OID = "course_id";
  public static final String UNIT_GOORU_OID = "unit_id";
  public static final String LESSON_GOORU_OID = "lesson_id";
  public static final String COLLECTION_OID = "collection_id";

  public static final String QUESTION_COUNT = "question_count";
  public static final String SESSION_ID = "session_id";
  public static final String COLLECTION_TYPE = "collection_type";
  public static final String RESOURCE_TYPE = "resource_type";
  public static final String QUESTION_TYPE = "question_type";
  public static final String ANSWER_OBJECT = "answer_object";
  public static final String RESOURCE_ID = "resource_id";

  public static final String TIMESPENT = "time_spent";
  public static final String VIEWS = "views";
  public static final String REACTION = "reaction";
  // (correct / incorrect / skipped / unevaluated)​
  public static final String RESOURCE_ATTEMPT_STATUS = "resource_attempt_status";
  public static final String SCORE = "score";
  // ********************************************
  public static final String CREATE_TIMESTAMP = "created_at";
  public static final String UPDATE_TIMESTAMP = "updated_at";

  public static final String APP_ID = "app_id";
  public static final String PARTNER_ID = "partner_id";
  public static final String COLLECTION_SUB_TYPE = "collection_sub_type";
  public static final String MAX_SCORE = "max_score";
  public static final String PATH_ID = "path_id";
  public static final String PATH_TYPE = "path_type";

  public static final String EVENT_ID = "event_id";
  public static final String TIME_ZONE = "time_zone";
  public static final String DATE_IN_TIME_ZONE = "date_in_time_zone";
  public static final String IS_GRADED = "is_graded";
  public static final String CONTENT_SOURCE = "content_source";
  public static final String ADDITIONAL_CONTEXT = "additional_context";
  public static final String DCA_CONTENT_ID = "dca_content_id";

  public static final String ATTR_TIME_SPENT = "timeSpent";
  public static final String ATTR_SCORE = "scoreInPercentage";
  public static final String ATTR_REACTION = "reaction";
  public static final String ATTR_COLLVIEWS = "views";
  public static final String ATTR_ATTEMPTS = "attempts";
  public static final String ATTR_CRP_EVENTNAME = "collection.resource.play";
  public static final String ATTR_CP_EVENTNAME = "collection.play";
  public static final String ATTR_EVENTTYPE_START = "start";
  public static final String ATTR_EVENTTYPE_STOP = "stop";
  public static final String ATTR_ASSESSMENT = "assessment";
  public static final String ATTR_COLLECTION = "collection";
  public static final String ATTR_COMPLETED_COUNT = "completedCount";
  public static final String ATTR_PEER_COUNT = "peerCount";
  public static final String ATTR_USERS = "users";
  public static final String ATTR_TOTALCOUNT = "totalCount";
  public static final String ATTR_COUNT = "count";

  public static final String ATTR_ASSESSMENT_ID = "assessmentId";
  public static final String ATTR_COLLECTION_ID = "collectionId";
  public static final String ATTR_ATTEMPT_STATUS = "attemptStatus";

  public static final String ATTR_TOTAL_COUNT = "totalCount";

  public static final String ATTR_CLASS_ID = "classId";
  public static final String ATTR_COURSE_ID = "courseId";
  public static final String ATTR_UNIT_ID = "unitId";
  public static final String ATTR_LESSON_ID = "lessonId";
  public static final String ATTR_RESOURCE_ID = "resourceId";
  public static final String ATTR_PATH_ID = "pathId";
  public static final String ATTR_COLLECTION_TYPE = "collectionType";
  public static final String ATTR_LAST_SESSION_ID = "lastSessionId";
  public static final String ATTR_PATH_TYPE = "pathType";
  // Teacher Grading - Rubrics
  public static final String ATTR_STUDENTS = "students";
  public static final String ATTR_LAST_ACCESSED = "lastAccessed";
  public static final String ATTR_ANSWER_TEXT = "answerText";
  public static final String ATTR_QUESTION_TEXT = "questionText";
  public static final String ATTR_QUESTION_ID = "questionId";
  public static final String ATTR_GRADE_STATUS = "gradingStatus";
  public static final String ATTR_MAX_SCORE = "maxScore";
  public static final String ATTR_SESSION_ID = "sessionId";
  public static final String ATTR_CONTENT_SOURCE = "contentSource";
  public static final String ATTR_DCA_CONTENT_ID = "dcaContentId";


  public static final String NA = "NA";
  public static final String AND = "AND";
  public static final String SPACE = " ";
  public static final String UNIT_ID = "unit_id = ? ";
  public static final String LESSON_ID = "lesson_id = ?";
  public static final String CLASS_ID = "class_id = ? ";
  public static final String DATE = "date";
  public static final String ACTIVITY_DATE = "activityDate";
  public static final String SUBMITTED_AT = "submittedAt";

  public static final String ASMT_TYPE_FILTER =
      " AND collection_type IN ('assessment','assessment-external') ";
  public static final String COLL_TYPE_FILTER =
      " AND collection_type IN ('collection', 'collection-external') ";

  // *****************************************************************************************************************************
  // Daily Class Activity

  public static final String SELECT_CLASS_COLLECTION_QUESTION_COUNT =
      "SELECT question_count,updated_at FROM daily_class_activity "
          + "WHERE class_id = ? AND collection_id = ? AND actor_id = ? AND event_name = 'collection.play'"
          + " ORDER BY updated_at DESC LIMIT 1";

  public static final String SELECT_DISTINCT_USERID_FOR_DAILY_CLASS_ACTIVITY =
      "SELECT DISTINCT(actor_id) FROM daily_class_activity " + "WHERE class_id = ?";

  public static final String GET_PERFORMANCE_FOR_CLASS_ASSESSMENTS =
      "SELECT SUM(agg.timeSpent) AS timeSpent, "
          + "(AVG(agg.scoreInPercentage)) scoreInPercentage, agg.lastSessionId, SUM(agg.attempts) AS attempts, "
          + "agg.collectionId, agg.activityDate FROM (SELECT time_spent AS timeSpent, "
          + "FIRST_VALUE(score) OVER (PARTITION BY collection_id, date_in_time_zone ORDER BY updated_at desc) AS scoreInPercentage, "
          + "FIRST_VALUE(session_id) OVER (PARTITION BY collection_id, date_in_time_zone ORDER BY updated_at desc) AS lastSessionId, "
          + "views AS attempts, collection_id as collectionId, actor_id as actorId, date_in_time_zone as activityDate FROM daily_class_activity "
          + "WHERE class_id = ? AND collection_id = ANY(?::varchar[]) AND actor_id = ? AND collection_type IN ('assessment', 'assessment-external') AND event_name = ? AND event_type = 'stop' "
          + "AND date_in_time_zone BETWEEN ? AND ?) AS agg GROUP BY agg.collectionId, agg.activityDate, agg.lastSessionId "
          + "ORDER BY agg.activityDate DESC";

  public static final String GET_PERFORMANCE_FOR_CLASS_COLLECTIONS =
      "SELECT SUM(CASE WHEN (agg.event_name = 'collection.resource.play' and agg.collection_type = 'collection') "
          + "THEN agg.timeSpent WHEN (agg.event_name = 'collection.play' and agg.collection_type = 'collection-external') THEN agg.timeSpent ELSE 0 END) AS timeSpent, "
          + "SUM(CASE WHEN (agg.event_name = 'collection.play') THEN agg.attempts ELSE 0 END) "
          + "AS attempts, agg.collectionId, agg.activityDate FROM (SELECT collection_type, time_spent AS timeSpent, views AS attempts, "
          + "collection_id as collectionId, actor_id as actorId, event_name, date_in_time_zone as activityDate "
          + "FROM daily_class_activity WHERE class_id = ? AND collection_id = ANY(?::varchar[]) AND actor_id = ? AND collection_type IN ('collection', 'collection-external') AND event_type = 'stop' "
          + "AND date_in_time_zone BETWEEN ? AND ? ) AS agg GROUP BY agg.collectionId, agg.activityDate, agg.collection_type ORDER BY agg.activityDate DESC";

  public static final String GET_PERFORMANCE_FOR_CLASS_COLLECTIONS_SCORE =
      "SELECT SUM(agg.score) AS score FROM " + "(SELECT DISTINCT ON (resource_id) collection_id, "
          + "FIRST_VALUE(score) OVER (PARTITION BY resource_id, date_in_time_zone ORDER BY updated_at desc) AS score "
          + "FROM daily_class_activity WHERE class_id = ? AND collection_id = ? AND actor_id = ? AND "
          + "event_name = 'collection.resource.play' AND resource_type = 'question' AND resource_attempt_status <> 'skipped' "
          + "AND date_in_time_zone = ?) AS agg " + "GROUP BY agg.collection_id";

  // GET STUDENT SESSION PERFORMANCE IN ASSESSMENTS
  public static final String SELECT_ASSESSMENT_PERF_FOR_SESSION_ID =
      "select distinct on (collection_id) FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_at desc) AS score,"
          + "collection_id,FIRST_VALUE(reaction) OVER (PARTITION BY collection_id ORDER BY updated_at desc) AS reaction,"
          + "FIRST_VALUE(time_spent) OVER (PARTITION BY collection_id ORDER BY updated_at desc) as time_spent,"
          + "updated_at,session_id,collection_type,FIRST_VALUE(views) OVER (PARTITION BY collection_id ORDER BY updated_at desc) AS collectionViews "
          + "from daily_class_activity WHERE session_id = ? AND actor_id = ? AND event_name = ? AND event_type = ?";

  public static final String SELECT_ASSESSMENT_REACTION_FOR_SESSION_ID =
      "SELECT round(avg(data.reaction)) as reaction FROM (SELECT DISTINCT ON (resource_id) collection_id, "
          + "FIRST_VALUE(reaction) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS reaction FROM daily_class_activity where session_id = ? AND actor_id = ? AND reaction > 0 "
          + "AND event_name = 'reaction.create') AS data group by data.collection_id;";

  public static final String SELECT_ASSESSMENT_QUESTION_FOR_SESSION_ID =
      "select distinct on (resource_id) FIRST_VALUE(score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS score, "
          + "resource_id, FIRST_VALUE(time_spent) OVER (PARTITION BY resource_id ORDER BY updated_at desc) as resourceTimeSpent,"
          + "updated_at, session_id, max_score, collection_type, resource_type, question_type, FIRST_VALUE(answer_object) OVER (PARTITION BY resource_id ORDER BY updated_at desc) as answer_object "
          + "from daily_class_activity WHERE session_id = ? AND actor_id = ? AND event_name = ? AND event_type = 'stop' ";

  public static final String SELECT_ASSESSMENT_RESOURCE_REACTION_FOR_SESSION_ID =
      " SELECT FIRST_VALUE(reaction) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS reaction "
          + "FROM daily_class_activity WHERE session_id = ? AND actor_id = ?  and resource_id = ? AND reaction > 0 AND event_name = 'reaction.create'";

  public static final String GET_OE_QUE_GRADE_STATUS_FOR_SESSION_ID =
      "SELECT is_graded FROM daily_class_activity WHERE session_id = ? AND actor_id = ?  and resource_id = ? AND event_name = 'collection.resource.play' AND event_type = 'stop'";

  // GET STUDENT SESSION PERF IN COLLECTION
  public static final String SELECT_COLLECTION_MAX_SCORE_FOR_SESSION =
      "SELECT SUM(agg.max_score) AS max_score FROM "
          + "(SELECT DISTINCT ON (resource_id) collection_id, FIRST_VALUE(updated_at) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS updated_at, "
          + "FIRST_VALUE(max_score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS max_score FROM daily_class_activity WHERE session_id = ? AND actor_id = ? AND "
          + "event_name = 'collection.resource.play' AND resource_type = 'question') AS agg GROUP BY agg.collection_id";

  public static final String SELECT_LAST_ACCESSED_TIME_OF_SESSION =
      "SELECT updated_at, session_id FROM daily_class_activity WHERE session_id = ? AND actor_id = ? AND event_name = 'collection.play' ORDER BY updated_at DESC LIMIT 1";

  public static final String SELECT_COLLECTION_AGG_DATA_FOR_SESSION =
      "SELECT SUM(CASE WHEN (agg.event_name = 'collection.resource.play' and agg.collection_type = 'collection') "
          + "THEN agg.time_spent WHEN  (agg.event_name = 'collection.play' and agg.collection_type = 'collection-external') THEN agg.time_spent ELSE 0 END) AS collectionTimeSpent, "
          + "SUM(CASE WHEN (agg.event_name = 'collection.play') THEN agg.views ELSE 0 END) AS collectionViews, "
          + "agg.collection_id, agg.completionStatus,agg.collection_type, 0 AS score, 0 AS reaction FROM (SELECT collection_id,collection_type,time_spent,session_id,views, event_name, "
          + "CASE  WHEN (FIRST_VALUE(event_type) OVER (PARTITION BY collection_id ORDER BY updated_at desc) = 'stop') THEN 'completed' ELSE 'in-progress' END AS completionStatus "
          + "FROM daily_class_activity WHERE event_name in ('collection.play', 'collection.resource.play') and session_id = ? AND actor_id = ?) AS agg "
          + "GROUP BY agg.collection_id,agg.completionStatus,agg.collection_type";

  public static final String SELECT_COLLECTION_AGG_SCORE_FOR_SESSION =
      "SELECT SUM(agg.score) AS score FROM (SELECT DISTINCT ON (resource_id) collection_id, FIRST_VALUE(score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS score "
          + "FROM daily_class_activity WHERE session_id = ? AND actor_id = ? AND event_name = 'collection.resource.play' AND resource_type = 'question' AND resource_attempt_status <> 'skipped' ) AS agg "
          + "GROUP BY agg.collection_id";

  public static final String SELECT_COLLECTION_AGG_REACTION_FOR_SESSION =
      "SELECT ROUND(AVG(agg.reaction)) AS reaction FROM (SELECT DISTINCT ON (resource_id) collection_id,  FIRST_VALUE(reaction) OVER (PARTITION BY resource_id ORDER BY updated_at desc) "
          + "AS reaction FROM daily_class_activity WHERE session_id = ? AND actor_id = ? AND event_name = 'reaction.create' AND reaction <> 0) AS agg GROUP BY agg.collection_id";

  public static final String SELECT_COLLECTION_RESOURCE_AGG_DATA_FOR_SESSION =
      "SELECT collection_id, resource_id ,resource_type,question_type, SUM(views) AS resourceViews, SUM(time_spent) AS resourceTimeSpent, 0 as reaction, 0 as score, '[]' AS answer_object "
          + "FROM daily_class_activity WHERE session_id = ? AND actor_id = ? AND event_name = 'collection.resource.play' GROUP BY collection_id, resource_id,resource_type,question_type";

  public static final String SELECT_COLLECTION_QUESTION_AGG_SCORE_FOR_SESSION =
      "SELECT DISTINCT ON (resource_id) FIRST_VALUE(score) OVER (PARTITION BY resource_id "
          + "ORDER BY updated_at desc) AS score,FIRST_VALUE(resource_attempt_status) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS attemptStatus, "
          + "FIRST_VALUE(answer_object) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS answer_object, "
          + "FIRST_VALUE(max_score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS max_score "
          + "FROM daily_class_activity WHERE session_id = ? AND actor_id = ? AND resource_id = ? AND event_name = 'collection.resource.play' AND resource_type = 'question' AND resource_attempt_status <> 'skipped'";

  // Getting RESOURCE DATA (reaction)
  public static final String SELECT_COLLECTION_RESOURCE_AGG_REACTION_FOR_SESSION =
      "SELECT DISTINCT ON (resource_id) " + "FIRST_VALUE(reaction) OVER (PARTITION BY resource_id "
          + "ORDER BY updated_at desc) AS reaction FROM daily_class_activity WHERE session_id = ? AND actor_id = ? AND resource_id = ?  AND event_name = 'reaction.create' AND reaction <> 0";

  // GET STUDENT PERFORMANCE SUMMARY IN ASSESSMENTS
  public static final String SELECT_ASSESSMENT_FOREACH_COLLID_AND_SESSION_ID =
      "select distinct on (collection_id) FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_at desc) AS score,"
          + "collection_id,FIRST_VALUE(reaction) OVER (PARTITION BY collection_id ORDER BY updated_at desc) AS reaction,"
          + "FIRST_VALUE(time_spent) OVER (PARTITION BY collection_id ORDER BY updated_at desc) as collectionTimeSpent,"
          + "updated_at,session_id,collection_type,FIRST_VALUE(views) OVER (PARTITION BY collection_id ORDER BY updated_at desc) AS collectionViews "
          + "from daily_class_activity WHERE collection_id = ? AND session_id = ? AND actor_id = ? AND date_in_time_zone = ? AND event_name = ? ";

  public static final String SELECT_ASSESSMENT_REACTION_AND_SESSION_ID =
      "SELECT round(avg(data.reaction)) as reaction FROM "
          + "(SELECT DISTINCT ON (resource_id) collection_id, "
          + "FIRST_VALUE(reaction) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS reaction "
          + "FROM daily_class_activity where collection_id = ? AND session_id = ? AND actor_id = ? AND reaction > 0 "
          + "AND event_name = 'reaction.create') AS data group by data.collection_id;";

  public static final String SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSION_ID_FOR_SUMMARY =
      "select  distinct on (resource_id) FIRST_VALUE(score * 100) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS score,"
          + "resource_id,FIRST_VALUE(reaction) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS reaction,"
          + "FIRST_VALUE(time_spent) OVER (PARTITION BY resource_id ORDER BY updated_at desc) as resourceTimeSpent,"
          + "updated_at,session_id,collection_type,"
          + "FIRST_VALUE(views) OVER (PARTITION BY resource_id ORDER BY updated_at asc) AS resourceViews, "
          + "resource_type,question_type,"
          + "FIRST_VALUE(answer_object) OVER (PARTITION BY resource_id ORDER BY updated_at desc) as answer_object "
          + "from daily_class_activity WHERE collection_id = ? AND session_id = ? AND event_name = ? ";

  public static final String SELECT_ASSESSMENT_RESOURCE_REACTION =
      " SELECT FIRST_VALUE(reaction) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS reaction "
          + "FROM daily_class_activity WHERE collection_id = ? AND session_id = ?  and resource_id = ? AND reaction > 0 AND event_name = 'reaction.create'";

  public static final String SELECT_CLASS_BY_SESSION_ID =
      "SELECT class_id FROM daily_class_activity WHERE collection_id = ? "
          + "AND session_id = ? AND class_id IS NOT NULL LIMIT 1";

  public static final String GET_ASMT_OE_QUE_GRADE_STATUS =
      "SELECT is_graded FROM daily_class_activity "
          + "WHERE collection_id = ? AND session_id = ?  and resource_id = ? AND event_name = 'collection.resource.play' "
          + "AND event_type = 'stop'";

  // Collection Summary report Queries
  public static final String SELECT_COLLECTION_QUESTION_COUNT =
      "SELECT question_count,updated_at FROM daily_class_activity "
          + "WHERE class_id = ? AND collection_id = ? AND actor_id = ? AND event_name = 'collection.play' AND date_in_time_zone = ? "
          + "ORDER BY updated_at DESC LIMIT 1";

  public static final String SELECT_COLLECTION_MAX_SCORE =
      "SELECT SUM(agg.max_score) AS max_score FROM "
          + "(SELECT DISTINCT ON (resource_id) collection_id, FIRST_VALUE(updated_at) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS updated_at, "
          + "FIRST_VALUE(max_score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS max_score "
          + "FROM daily_class_activity WHERE class_id = ? AND collection_id = ? AND actor_id = ? AND date_in_time_zone = ? AND "
          + "event_name = 'collection.resource.play' AND resource_type = 'question') AS agg "
          + "GROUP BY agg.collection_id";

  // Getting COLLECTION DATA (views, time_spent)
  public static final String SELECT_COLLECTION_AGG_DATA =
      "SELECT SUM(CASE WHEN (agg.event_name = 'collection.resource.play' and agg.collection_type = 'collection') "
          + "THEN agg.time_spent WHEN  (agg.event_name = 'collection.play' and agg.collection_type = 'collection-external') THEN agg.time_spent ELSE 0 END) AS collectionTimeSpent, "
          + "SUM(CASE WHEN (agg.event_name = 'collection.play') THEN agg.views ELSE 0 END) AS collectionViews, "
          + "agg.collection_id, agg.completionStatus,agg.collection_type, 0 AS score, 0 AS reaction FROM "
          + "(SELECT collection_id,collection_type,time_spent,session_id,views, event_name, "
          + "CASE  WHEN (FIRST_VALUE(event_type) OVER (PARTITION BY collection_id ORDER BY updated_at desc) = 'stop') THEN 'completed' ELSE 'in-progress' END AS completionStatus "
          + "FROM daily_class_activity WHERE event_name in ('collection.play', 'collection.resource.play') and class_id = ? AND collection_id = ? AND actor_id = ? AND date_in_time_zone = ?) AS agg "
          + "GROUP BY agg.collection_id,agg.completionStatus,agg.collection_type";

  // Getting COLLECTION DATA (score)
  public static final String SELECT_COLLECTION_AGG_SCORE = "SELECT SUM(agg.score) AS score FROM "
      + "(SELECT DISTINCT ON (resource_id) collection_id, "
      + "FIRST_VALUE(score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS score "
      + "FROM daily_class_activity WHERE class_id = ? AND collection_id = ? AND actor_id = ? AND date_in_time_zone = ? AND "
      + "event_name = 'collection.resource.play' AND resource_type = 'question' AND resource_attempt_status <> 'skipped' ) AS agg "
      + "GROUP BY agg.collection_id";

  // Getting COLLECTION DATA (reaction)
  public static final String SELECT_COLLECTION_AGG_REACTION =
      "SELECT ROUND(AVG(agg.reaction)) AS reaction "
          + "FROM (SELECT DISTINCT ON (resource_id) collection_id,  "
          + "FIRST_VALUE(reaction) OVER (PARTITION BY resource_id ORDER BY updated_at desc) "
          + "AS reaction FROM daily_class_activity " + "WHERE class_id = ? AND collection_id = ? "
          + "AND actor_id = ? AND date_in_time_zone = ? AND event_name = 'reaction.create' AND reaction <> 0) AS agg GROUP BY agg.collection_id";

  // Getting RESOURCE DATA (views, time_spent)
  public static final String SELECT_COLLECTION_RESOURCE_AGG_DATA =
      "SELECT collection_id, resource_id ,resource_type,question_type, SUM(views) AS resourceViews, "
          + "SUM(time_spent) AS resourceTimeSpent, 0 as reaction, 0 as score, '[]' AS answer_object "
          + "FROM daily_class_activity WHERE class_id = ? AND collection_id = ? "
          + "AND actor_id = ? AND date_in_time_zone = ? AND event_name = 'collection.resource.play' GROUP BY collection_id, resource_id,resource_type,question_type";

  public static final String SELECT_COLLECTION_QUESTION_AGG_SCORE =
      "SELECT DISTINCT ON (resource_id) " + "FIRST_VALUE(score) OVER (PARTITION BY resource_id "
          + "ORDER BY updated_at desc) AS score,FIRST_VALUE(resource_attempt_status) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS attemptStatus, "
          + "FIRST_VALUE(answer_object) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS answer_object,"
          + "FIRST_VALUE(session_id) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS session_id, "
          + "FIRST_VALUE(max_score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS max_score "
          + "FROM daily_class_activity WHERE class_id = ? AND collection_id = ? AND resource_id = ? "
          + "AND actor_id = ? AND date_in_time_zone = ? AND event_name = 'collection.resource.play' AND resource_type = 'question' "
          + "AND resource_attempt_status <> 'skipped'";


  // Getting RESOURCE DATA (reaction)
  public static final String SELECT_COLLECTION_RESOURCE_AGG_REACTION =
      "SELECT DISTINCT ON (resource_id) " + "FIRST_VALUE(reaction) OVER (PARTITION BY resource_id "
          + "ORDER BY updated_at desc) AS reaction "
          + "FROM daily_class_activity WHERE class_id = ? AND collection_id = ? AND resource_id = ? "
          + "AND actor_id = ? AND date_in_time_zone = ? AND event_name = 'reaction.create' AND reaction <> 0";

  public static final String SELECT_COLLECTION_LAST_ACCESSED_TIME =
      "SELECT updated_at, session_id FROM daily_class_activity "
          + "WHERE class_id = ? AND collection_id = ? AND actor_id = ? AND date_in_time_zone = ? AND event_name = 'collection.play'"
          + " ORDER BY updated_at DESC LIMIT 1";

  public static final String GET_COLL_OE_QUE_GRADE_STATUS =
      "SELECT is_graded FROM daily_class_activity "
          + "WHERE class_id = ? AND collection_id = ? AND resource_id = ? "
          + "AND actor_id = ? AND date_in_time_zone = ? AND event_name = 'collection.resource.play' AND event_type = 'stop'";


  // ***************************************************************************************
  // STUDENT PERFORMANCE in Assessment
  // public static final String SELECT_DISTINCT_USERID_FOR_ASSESSMENT_ID_FILTERBY_COLLTYPE =
  // "SELECT DISTINCT(actor_id) FROM daily_class_activity "
  // + "WHERE class_id = ? AND collection_id = ? AND collection_type =? AND date_in_time_zone
  // BETWEEN ? AND ?";

  public static final String SELECT_DISTINCT_USERID_FOR_ASSESSMENT =
      "SELECT DISTINCT(actor_id) FROM daily_class_activity "
          + "WHERE class_id = ? AND collection_id = ? AND date_in_time_zone = ? "
          + "AND event_name = ? AND event_type = ? ";

  public static final String SELECT_ASSESSMENT_REACTION =
      "SELECT round(avg(data.reaction)) as reaction FROM "
          + "(SELECT DISTINCT ON (resource_id) collection_id, "
          + "FIRST_VALUE(reaction) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS reaction "
          + "FROM daily_class_activity where collection_id = ? AND session_id = ? AND date_in_time_zone = ? AND reaction > 0 "
          + "AND event_name = 'reaction.create') AS data group by data.collection_id;";


  public static final String GET_LATEST_COMPLETED_SESSION_ID =
      "SELECT session_id FROM daily_class_activity WHERE "
          + " class_id = ? AND collection_id = ? AND actor_id = ? AND event_name = 'collection.play' AND event_type = 'stop' "
          + " AND date_in_time_zone BETWEEN ? AND ? ORDER BY created_at DESC LIMIT 1";

  // Reactions need not be included in these queries, since that should be obtained from separate
  // event
  public static final String SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSION_ID =
      "select distinct on (resource_id) FIRST_VALUE(score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS score, "
          + "resource_id, FIRST_VALUE(time_spent) OVER (PARTITION BY resource_id ORDER BY updated_at desc) as resourceTimeSpent,"
          + "updated_at, session_id, max_score, collection_type, resource_type, question_type, "
          + "FIRST_VALUE(answer_object) OVER (PARTITION BY resource_id ORDER BY updated_at desc) as answer_object "
          + "from daily_class_activity WHERE collection_id = ? AND session_id = ? AND date_in_time_zone = ? AND event_name = ? AND event_type = 'stop' ";

  // TODO: Include actor_id
  public static final String GET_OE_QUE_GRADE_STATUS = "SELECT is_graded FROM daily_class_activity "
      + "WHERE collection_id = ? AND session_id = ?  and resource_id = ? AND date_in_time_zone = ? AND event_name = 'collection.resource.play' "
      + "AND event_type = 'stop'";

  // *****************************************************************************************************************************
  // Collection Performance Report Queries

  // STUDENT PERFORMANCE in Assessment
  public static final String SELECT_DISTINCT_USERID_FOR_COLLECTION_ID =
      "SELECT DISTINCT(actor_id) FROM daily_class_activity "
          + "WHERE class_id = ? AND collection_id = ? AND date_in_time_zone = ?";

  // **************************************************************************************************************************************************

  // GET SESSION STATUS
  public static final String GET_SESSION_STATUS =
      "SELECT event_name, event_type, updated_at from daily_class_activity WHERE session_id = ? "
          + " AND collection_id = ? AND event_name = ? ";

  // GET USER ALL SESSIONS FROM ASSESSMENT
  public static final String GET_ASMT_USER_SESSIONS_FOR_COLLID =
      "SELECT DISTINCT s.session_id, s.updated_at FROM "
          + "(SELECT FIRST_VALUE(updated_at) OVER (PARTITION BY session_id ORDER BY updated_at DESC) AS updated_at, session_id "
          + "FROM daily_class_activity WHERE class_id = ? AND collection_id = ? AND collection_type IN ('assessment', 'assessment-external') AND actor_id = ? "
          + "AND date_in_time_zone BETWEEN ? AND ? ) AS s ORDER BY s.updated_at ASC";

  // *************************************************************************************************************************


  public static final String SELECT_DISTINCT_USERID_FOR_DCA =
      "SELECT DISTINCT(actor_id) FROM daily_class_activity "
          + "WHERE class_id = ?  AND collection_type =? AND extract(year from updated_at) = ? ";

  public static final String DCA_MONTHLY_USAGE_ASSESSMENT_AGG_DATA =
      "SELECT assessmentData.month as month, max(score) as score, "
          + "sum(time_spent) as time_spent FROM (select FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_at desc) "
          + "AS score, time_spent,to_char(updated_at,'Mon') as month FROM daily_class_activity WHERE class_id = ? AND actor_id = ? "
          + "AND event_name = 'collection.play' AND collection_type = 'assessment' AND event_type = 'stop' "
          + "AND extract(year from updated_at) = ?) as assessmentData group by month";

  public static final String DCA_MONTHLY_USAGE_ASSESSMENT_DATA =
      "select collection_id, max(score) as score, sum(time_spent) as time_spent from "
          + "(select collection_id, FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_at desc) AS score, "
          + "time_spent FROM daily_class_activity WHERE class_id = ? AND actor_id = ? AND event_name = 'collection.play' AND "
          + "collection_type = 'assessment' AND event_type = 'stop' AND extract(year from updated_at) = ? "
          + "AND to_char(updated_at,'Mon') =  ?) as assessmentData group by collection_id";

  public static final String DCA_WEEKLY_USAGE_ASSESSMENT_AGG_DATA =
      "SELECT assessmentData.week as week, max(score) as score, "
          + "SUM(time_spent) as time_spent FROM (select FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_at desc) "
          + "AS score, time_spent,extract(week from updated_at) as week FROM daily_class_activity WHERE class_id = ? AND actor_id = ? "
          + "AND event_name = 'collection.play' AND collection_type = 'assessment' "
          + "AND event_type = 'stop' AND extract(year from updated_at) = ? AND to_char(updated_at,'Mon') =  ?) "
          + "AS assessmentData group by week";

  public static final String DCA_WEEKLY_USAGE_ASSESSMENT_DATA =
      "select collection_id, max(score) as score, sum(time_spent) "
          + "as time_spent FROM (select collection_id, FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_at desc) "
          + "AS score, time_spent FROM daily_class_activity WHERE class_id = ? AND actor_id = ? AND event_name = 'collection.play' AND "
          + "collection_type = 'assessment' AND event_type = 'stop' AND extract(year from updated_at) = ? AND "
          + "to_char(updated_at,'Mon') =  ? AND extract(week from updated_at) = ?) as assessmentData group by collection_id";

  public static final String DCA_MONTHLY_USAGE_COLLECTION_AGG_DATA =
      "SELECT collectionData.month, SUM(collectionData.time_spent) "
          + "as time_spent, SUM(collectionData.score) AS score, SUM(collectionData.max_score) AS max_score "
          + "FROM (SELECT DISTINCT ON (resource_id) collection_id,resource_type,time_spent, "
          + "FIRST_VALUE(score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS score, "
          + "FIRST_VALUE(max_score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS max_score, "
          + "to_char(updated_at,'Mon') as month FROM daily_class_activity WHERE class_id = ? actor_id = ? "
          + "AND event_name = 'collection.resource.play' AND extract(year from updated_at) = ?) AS collectionData "
          + "GROUP BY collectionData.month";

  public static final String DCA_MONTHLY_USAGE_COLLECTION_DATA =
      "SELECT collectionData.collection_id, SUM (collectionData.time_spent) "
          + "as time_spent, SUM(collectionData.score) AS score, SUM(collectionData.max_score) AS max_score "
          + "FROM (SELECT DISTINCT ON (resource_id) collection_id,time_spent, "
          + "FIRST_VALUE(score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS score, "
          + "FIRST_VALUE(max_score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS max_score, "
          + "to_char(updated_at,'Mon') as month FROM daily_class_activity "
          + "WHERE class_id = ? actor_id = ? AND event_name = 'collection.resource.play' "
          + "AND extract(year from updated_at) = ? AND to_char(updated_at,'Mon') =  ?) AS collectionData "
          + "GROUP BY collectionData.collection_id";

  public static final String DCA_WEEKLY_USAGE_COLLECTION_AGG_DATA =
      "SELECT collectionData.week, SUM(collectionData.time_spent) "
          + "AS time_spent, SUM(collectionData.score) AS score, SUM(collectionData.max_score) AS max_score "
          + "FROM (SELECT DISTINCT ON (resource_id) collection_id,resource_type,time_spent, "
          + "FIRST_VALUE(score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS score, "
          + "FIRST_VALUE(max_score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS max_score, "
          + "extract(week from updated_at) as week FROM daily_class_activity WHERE class_id = ? actor_id = ? "
          + "AND event_name = 'collection.resource.play' AND extract(year from updated_at) = ? AND "
          + "to_char(updated_at,'Mon') =  ?) AS collectionData GROUP BY collectionData.week";

  public static final String DCA_WEEKLY_USAGE_COLLECTION_DATA =
      "SELECT collectionData.collection_id, SUM (collectionData.time_spent) "
          + "as time_spent, SUM(collectionData.score) AS score, SUM(collectionData.max_score) AS max_score "
          + "FROM (SELECT DISTINCT ON (resource_id) collection_id,time_spent, "
          + "FIRST_VALUE(score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS score, "
          + "FIRST_VALUE(max_score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS max_score, "
          + "to_char(updated_at,'Mon') as month FROM daily_class_activity "
          + "WHERE class_id = ? actor_id = ? AND event_name = 'collection.resource.play' "
          + "AND extract(year from updated_at) = ? AND to_char(updated_at,'Mon') =  ? AND extract(week from updated_at) = ?) "
          + "AS collectionData GROUP BY collectionData.collection_id";
  // ************************************************************************************************************************
  public static final String DCA_CLASS_SCORE_YEAR_MONTH_BREAKDOWN =
      "SELECT EXTRACT(YEAR FROM date_in_time_zone) AS year, EXTRACT(MONTH FROM date_in_time_zone) AS month, AVG(score) as score FROM daily_class_activity WHERE class_id = ? AND event_name = 'collection.play' AND collection_type IN ('assessment','assessment-external') AND event_type = 'stop' group by year, month order by year desc, month asc;";

  public static final String DCA_CLASS_SCORE_YEAR_MONTH_BREAKDOWN_FOR_USER =
      "SELECT EXTRACT(YEAR FROM date_in_time_zone) AS year, EXTRACT(MONTH FROM date_in_time_zone) AS month, AVG(score) as score FROM daily_class_activity WHERE class_id = ? AND event_name = 'collection.play' AND collection_type IN ('assessment','assessment-external') AND event_type = 'stop' AND actor_id = ? group by year, month order by year desc, month asc;";

  public static final String DCA_CLASS_TS_SUMMARY_FOR_MONTH =
      "SELECT ROUND(AVG(time_spent)) AS time_spent FROM (SELECT collection_id, ROUND(AVG(time_spent)) AS time_spent FROM (SELECT actor_id, collection_id, SUM(time_spent) as time_spent FROM daily_class_activity WHERE class_id = ? AND event_name = 'collection.resource.play' AND collection_type IN ('collection', 'collection-external') AND extract(year from date_in_time_zone) = ? AND extract(month from date_in_time_zone) = ? GROUP BY actor_id, collection_id) ca group by collection_id) c";

  public static final String DCA_CLASS_TS_SUMMARY_FOR_MONTH_FOR_USER =
      "SELECT ROUND(AVG(time_spent)) AS time_spent FROM (SELECT collection_id, ROUND(AVG(time_spent)) AS time_spent FROM (SELECT actor_id, collection_id, SUM(time_spent) as time_spent FROM daily_class_activity WHERE class_id = ? AND event_name = 'collection.resource.play' AND collection_type IN ('collection', 'collection-external') AND extract(year from date_in_time_zone) = ? AND extract(month from date_in_time_zone) = ? AND actor_id = ? GROUP BY actor_id, collection_id) ca group by collection_id) c";

  public static final String DCA_CLASS_ASMT_SUMMARY_FOR_MONTH =
      "SELECT collection_id, collection_type, AVG(score) AS score, ROUND(AVG(time_spent)) AS time_spent FROM daily_class_activity WHERE class_id = ? AND event_name = 'collection.play' AND collection_type IN ('assessment','assessment-external') AND event_type = 'stop' AND extract(year from date_in_time_zone) = ? AND extract(month from date_in_time_zone) = ? group by collection_id, collection_type";

  public static final String DCA_CLASS_ASMT_SUMMARY_FOR_MONTH_FOR_USER =
      "SELECT collection_id, collection_type, AVG(score) AS score, ROUND(AVG(time_spent)) AS time_spent FROM daily_class_activity WHERE class_id = ? AND event_name = 'collection.play' AND collection_type IN ('assessment','assessment-external') AND event_type = 'stop' AND extract(year from date_in_time_zone) = ? AND extract(month from date_in_time_zone) = ? AND actor_id = ? group by collection_id, collection_type";

  public static final String DCA_CLASS_COLL_SUMMARY_FOR_MONTH =
      "SELECT collection_id, collection_type, ROUND(AVG (time_spent)) AS time_spent FROM (SELECT actor_id, collection_id, collection_type, SUM(time_spent) as time_spent FROM daily_class_activity WHERE class_id = ? AND event_name = 'collection.resource.play' AND collection_type IN ('collection', 'collection-external') AND extract(year from date_in_time_zone) = ? AND extract(month from date_in_time_zone) =  ? GROUP BY actor_id, collection_id, collection_type) a GROUP by collection_id, collection_type";

  public static final String DCA_CLASS_COLL_SUMMARY_FOR_MONTH_FOR_USER =
      "SELECT collection_id, collection_type, ROUND(AVG (time_spent)) AS time_spent FROM (SELECT actor_id, collection_id, collection_type, SUM(time_spent) as time_spent FROM daily_class_activity WHERE class_id = ? AND event_name = 'collection.resource.play' AND collection_type IN ('collection', 'collection-external') AND extract(year from date_in_time_zone) = ? AND extract(month from date_in_time_zone) =  ? AND actor_id = ? GROUP BY actor_id, collection_id, collection_type) a GROUP by collection_id, collection_type";

  public static final String DCA_CLASS_USER_USAGE_ASSESSMENT_DATA =
      "SELECT actor_id, AVG(score) as score, ROUND(AVG(time_spent)) AS time_spent FROM daily_class_activity WHERE class_id = ? AND collection_id = ? AND event_name = 'collection.play' AND event_type = 'stop' AND extract(year from date_in_time_zone) = ? AND extract(month from date_in_time_zone) =  ? group by actor_id";

  public static final String DCA_CLASS_USER_USAGE_COLLECTION_DATA =
      "SELECT actor_id, ROUND(AVG(time_spent)) AS time_spent FROM (SELECT actor_id, collection_id, SUM (time_spent) as time_spent FROM daily_class_activity WHERE class_id = ? AND collection_id = ? AND event_name = 'collection.resource.play' AND extract(year from date_in_time_zone) = ? AND extract(month from date_in_time_zone) =  ? GROUP BY actor_id, collection_id) a GROUP by actor_id";
  // *************************************************************************************************************************
  // Student Class performance
  public static final String SELECT_STUDENT_CLASS_COMPLETION_SCORE =
      "SELECT AVG(score) AS scoreInPercentage, count(*) as completedCount "
          + "FROM daily_class_activity WHERE class_id = ? AND actor_id = ? "
          + "AND event_name = 'collection.play' AND event_type = 'stop' AND collection_type IN ('assessment', 'assessment-external') "
          + "AND (path_id IS NULL OR path_id = 0) AND score IS NOT NULL";

  public static final String SELECT_STUDENT_CLASSES_COMPLETION_SCORE =
      "SELECT class_id, AVG(score) AS scoreInPercentage, count(*) as completedCount "
          + "FROM daily_class_activity WHERE class_id = ANY(?::varchar[]) AND actor_id = ? "
          + "AND event_name = 'collection.play' AND event_type = 'stop' AND collection_type IN ('assessment', 'assessment-external') "
          + "AND (path_id IS NULL OR path_id = 0) AND score IS NOT NULL GROUP BY class_id, actor_id";

  public static final String GET_DISTINCT_USERS_IN_CLASS =
      "select distinct(actor_id) from daily_class_activity where collection_type IN "
          + "('assessment', 'assessment-external') AND class_id = ? and event_name =  'collection.play' and event_type = 'stop' "
          + "AND (path_id IS NULL OR path_id = 0)";

  public static final String SELECT_ALL_STUDENT_CLASS_COMPLETION_SCORE =
      "select AVG(score) as scoreInPercentage, count(*) as completedCount "
          + "from daily_class_activity where collection_type IN ('assessment', 'assessment-external') and actor_id = ANY(?::varchar[]) and "
          + "event_name = 'collection.play' and event_type = 'stop' and class_id = ? AND (path_id IS NULL OR path_id = 0)";

  public static final String SELECT_CLASS_COMPLETION_SCORE_FOR_TEACHER =
      "select class_id, AVG(score) as scoreInPercentage, count(*) as completedCount "
          + "from daily_class_activity where collection_type IN ('assessment', 'assessment-external') and "
          + "event_name = 'collection.play' and event_type = 'stop' and class_id = ANY(?::varchar[]) "
          + "AND (path_id IS NULL OR path_id = 0) GROUP BY class_id";
  // *************************************************************************************************************************
  // RUBRIC GRADING
  public static final String GET_QUESTIONS_TO_GRADE =
      "SELECT distinct on (resource_id, collection_id, actor_id, date_in_time_zone) session_id, "
          + "collection_id, collection_type, resource_id, actor_id, date_in_time_zone from daily_class_activity where class_id = ? AND "
          + "event_name = 'collection.resource.play' AND event_type = 'stop' AND resource_type = 'question' "
          + "AND is_graded = 'false' AND resource_attempt_status = 'attempted' AND grading_type = 'teacher' AND question_type = 'OE' "
          + "order by resource_id, collection_id, actor_id, date_in_time_zone, updated_at desc";

  public static final String GET_DISTINCT_STUDENTS_FOR_THIS_RESOURCE =
      "SELECT distinct (actor_id) from daily_class_activity where "
          + "class_id = ? AND collection_id = ? AND resource_id = ? AND event_type = 'stop' AND "
          + "event_name = 'collection.resource.play' AND resource_type = 'question' "
          + "AND is_graded = 'false' AND resource_attempt_status = 'attempted' AND grading_type = 'teacher'  "
          + "AND question_type = 'OE' AND date_in_time_zone = ?";

  public static final String GET_LATEST_SCORE_FOR_THIS_RESOURCE_STUDENT =
      "SELECT score, is_graded, resource_id from daily_class_activity "
          + "WHERE class_id = ? AND collection_id = ? AND resource_id = ? AND actor_id = ? "
          + "AND event_name = 'collection.resource.play' AND event_type = 'stop' AND resource_type = 'question' "
          + "AND grading_type = 'teacher' AND  date_in_time_zone = ? AND question_type = 'OE' order by updated_at desc LIMIT 1";

  public static final String GET_STUDENTS_ANSWER_FOR_RUBRIC_QUESTION =
      "SELECT score, answer_object AS answerText, time_spent, session_id, "
          + "resource_id, updated_at, actor_id from daily_class_activity "
          + "where class_id = ? AND collection_id = ? AND resource_id = ? AND actor_id = ? AND date_in_time_zone = ? AND "
          + "event_name = 'collection.resource.play' AND event_type = 'stop' AND "
          + "resource_type = 'question' AND is_graded = 'false' AND resource_attempt_status = 'attempted' AND "
          + "grading_type = 'teacher' AND question_type = 'OE' order by updated_at desc LIMIT 1";

  
  public static final String GET_OA_TO_GRADE =
      "SELECT dca_content_id, collection_id, collection_type, actor_id, date_in_time_zone "
      + "from daily_class_activity where class_id = ? AND event_name = 'collection.play' AND event_type = 'stop' "
      + "AND collection_type = 'offline-activity' AND is_graded = 'false' AND grading_type = 'teacher' "
      + "order by collection_id, actor_id, date_in_time_zone, updated_at desc";

  public static final String GET_OA_PENDING_GRADING =
      "actor_id = ? AND class_id = ? AND collection_id = ? AND event_name = 'collection.play' "
          + "AND event_type = 'stop' AND date_in_time_zone = ? ORDER BY updated_at DESC";

  public static final String GET_DISTINCT_STUDENTS_FOR_THIS_OA =
      "SELECT distinct (actor_id) from daily_class_activity where class_id = ? AND dca_content_id = ? "
      + "AND event_type = 'stop' AND event_name = 'collection.play' AND collection_type = 'offline-activity' "
      + "AND is_graded = 'false' AND grading_type = 'teacher'";
  
  public static final String GET_OA_STUDENTS_PENDING_GRADING = "class_id = ? AND dca_content_id = ? AND actor_id = ? "
      + "AND event_name = 'collection.play' AND event_type = 'stop' AND collection_type = 'offline-activity' "
      + "AND grading_type = 'teacher' AND is_graded = false order by updated_at desc";
  
  public static final String UUID_TYPE = "uuid";
  public static Pattern YEAR_PATTERN = Pattern.compile("^\\d{4}$");

}
