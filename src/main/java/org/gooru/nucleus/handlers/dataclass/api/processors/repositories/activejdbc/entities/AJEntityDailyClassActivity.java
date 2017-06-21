package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mukul@gooru
 * 
 */

@Table("daily_class_activity")
public class AJEntityDailyClassActivity extends Model{


  	private static final Logger LOGGER = LoggerFactory.getLogger(AJEntityDailyClassActivity.class);
  	public static final String ID = "id";
  	public static final String EVENTNAME = "event_name";
  	
  	public static final String EVENTTYPE = "event_type";
  	//actor_id is userId or gooruuid
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
    //(correct / incorrect / skipped / unevaluated)​
    public static final String RESOURCE_ATTEMPT_STATUS = "resource_attempt_status";    
    public static final String SCORE = "score";
    //********************************************
    public static final String CREATE_TIMESTAMP = "created_at";
    public static final String UPDATE_TIMESTAMP = "updated_at";  
    
    public static final String APP_ID = "app_id";
    public static final String PARTNER_ID = "partner_id";
    public static final String COLLECTION_SUB_TYPE = "collection_sub_type";
    public static final String MAX_SCORE = "max_score";
    public static final String PATH_ID = "path_id";

    
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
    //Need to Segregate Assessment and Collection in the Sourcelist: Json of Unit Perf
    public static final String ATTR_ASSESSMENT_ID = "assessmentId";
    public static final String ATTR_COLLECTION_ID = "collectionId";
    public static final String ATTR_ATTEMPT_STATUS = "attemptStatus";
    //Attributes for which values are not available, stuff NA into Json, eg. TotalCount
    public static final String ATTR_TOTAL_COUNT = "totalCount";
    
    public static final String ATTR_CLASS_ID = "classId";
    public static final String ATTR_COURSE_ID = "courseId";
    public static final String ATTR_UNIT_ID = "unitId";
    public static final String ATTR_LESSON_ID = "lessonId";    
    public static final String ATTR_RESOURCE_ID = "resourceId";
    public static final String ATTR_PATH_ID = "pathId";
    public static final String ATTR_COLLECTION_TYPE = "collectionType";    
    public static final String ATTR_LAST_SESSION_ID = "lastSessionId";

    public static final String NA = "NA";
    public static final String AND = "AND";
    public static final String SPACE = " ";
    public static final String UNIT_ID = "unit_id = ? ";
    public static final String LESSON_ID = "lesson_id = ?";
    public static final String CLASS_ID = "class_id = ? ";
    public static final String DATE = "date";
    public static final String ACTIVITY_DATE = "activityDate";

    //*****************************************************************************************************************************
    //Daily Class Activity
    
    public static final String SELECT_CLASS_COLLECTION_QUESTION_COUNT = "SELECT question_count,updated_at FROM daily_class_activity "
            + "WHERE class_id = ? AND collection_id = ? AND actor_id = ? AND event_name = 'collection.play'"
            + " ORDER BY updated_at DESC LIMIT 1";
    
    public static final String SELECT_DISTINCT_USERID_FOR_DAILY_CLASS_ACTIVITY =
            "SELECT DISTINCT(actor_id) FROM daily_class_activity "
            + "WHERE class_id = ? AND collection_type = ?";
    
    public static final String GET_PERFORMANCE_FOR_CLASS_ASSESSMENTS = "SELECT SUM(agg.timeSpent) AS timeSpent, "
            + "(AVG(agg.scoreInPercentage)) scoreInPercentage, agg.lastSessionId, SUM(agg.attempts) AS attempts, "
            + "agg.collectionId, agg.activityDate FROM (SELECT time_spent AS timeSpent, "
            + "FIRST_VALUE(score) OVER (PARTITION BY collection_id, DATE(updated_at) ORDER BY updated_at desc) AS scoreInPercentage, "
            + "FIRST_VALUE(session_id) OVER (PARTITION BY collection_id, DATE(updated_at) ORDER BY updated_at desc) AS lastSessionId, "
            + "views AS attempts, collection_id as collectionId,actor_id as actorId,DATE(updated_at) as activityDate FROM daily_class_activity "
            + "WHERE class_id = ? AND collection_id = ANY(?::varchar[]) AND actor_id = ? AND collection_type = ? AND event_name = ? AND event_type = 'stop' "
            + "AND DATE(updated_at) BETWEEN ? AND ?) AS agg GROUP BY agg.collectionId, agg.activityDate, agg.lastSessionId "
            + "ORDER BY agg.activityDate DESC";    
    
    public static final String GET_PERFORMANCE_FOR_CLASS_COLLECTIONS = "SELECT SUM(CASE WHEN (agg.event_name = 'collection.resource.play') "
    		+ "THEN agg.timeSpent ELSE 0 END) AS timeSpent, SUM(CASE WHEN (agg.event_name = 'collection.play') THEN agg.attempts ELSE 0 END) "
    		+ "AS attempts, agg.collectionId, agg.activityDate FROM (SELECT time_spent AS timeSpent, views AS attempts, "
    		+ "collection_id as collectionId, actor_id as actorId, event_name, DATE(updated_at) as activityDate "
    		+ "FROM daily_class_activity WHERE class_id = ? AND collection_id = ANY(?::varchar[]) AND actor_id = ? AND collection_type = ? AND event_type = 'stop' "
    		+ "AND DATE (updated_at) BETWEEN ? AND ? ) AS agg GROUP BY agg.collectionId, agg.activityDate ORDER BY agg.activityDate DESC";
    
    public static final String GET_PERFORMANCE_FOR_CLASS_COLLECTIONS_SCORE = "SELECT SUM(agg.score) AS score FROM "
            + "(SELECT DISTINCT ON (resource_id) collection_id, "
            + "FIRST_VALUE(score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS score "
            + "FROM daily_class_activity WHERE class_id = ? AND collection_id = ? AND actor_id = ? AND "
            + "event_name = 'collection.resource.play' AND resource_type = 'question' AND resource_attempt_status <> 'skipped' ) AS agg "
            + "GROUP BY agg.collection_id";
    
    public static final String GET_PERFORMANCE_FOR_ASSESSMENTS =
            "SELECT SUM(agg.time_spent) timeSpent, (AVG(agg.scoreInPercentage)) scoreInPercentage, "
          + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.collection_id AS collectionId, 'completed' AS attemptStatus "
          + "FROM (SELECT time_spent, FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_at desc) "
          + "AS scoreInPercentage, reaction AS reaction, views AS attempts, collection_id FROM daily_class_activity "
          + "WHERE collection_id = ANY(?::varchar[]) AND actor_id = ? AND "
          + "event_name = ? AND event_type = 'stop') AS agg "
          + "GROUP BY agg.collection_id";
   
    //GET STUDENT PERFORMANCE SUMMARY IN ASSESSMENTS    
    public static final String SELECT_ASSESSMENT_FOREACH_COLLID_AND_SESSION_ID =
            "select distinct on (collection_id) FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_at desc) AS score,"
            + "collection_id,FIRST_VALUE(reaction) OVER (PARTITION BY collection_id ORDER BY updated_at desc) AS reaction,"
            + "FIRST_VALUE(time_spent) OVER (PARTITION BY collection_id ORDER BY updated_at desc) as collectionTimeSpent,"
            + "updated_at,session_id,collection_type,FIRST_VALUE(views) OVER (PARTITION BY collection_id ORDER BY updated_at desc) AS collectionViews "
            + "from daily_class_activity WHERE collection_id = ? AND session_id = ? AND event_name = ? ";
    
    public static final String SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSION_ID =
            "select  distinct on (resource_id) FIRST_VALUE(score * 100) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS score,"
            + "resource_id,FIRST_VALUE(reaction) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS reaction,"
            + "FIRST_VALUE(time_spent) OVER (PARTITION BY resource_id ORDER BY updated_at desc) as resourceTimeSpent,"
            + "updated_at,session_id,collection_type,"
            + "FIRST_VALUE(views) OVER (PARTITION BY resource_id ORDER BY updated_at asc) AS resourceViews, "
            + "resource_type,question_type,"
            + "FIRST_VALUE(answer_object) OVER (PARTITION BY resource_id ORDER BY updated_at desc) as answer_object "
            + "from daily_class_activity WHERE collection_id = ? AND session_id = ? AND event_name = ? ";
    
    public static final String SELECT_CLASS_BY_SESSION_ID = "SELECT class_id FROM daily_class_activity WHERE collection_id = ? "
    		+ "AND session_id = ? AND class_id IS NOT NULL LIMIT 1";

    //Collection Summary report Queries
    //Getting collection question count
    public static final String SELECT_COLLECTION_QUESTION_COUNT = "SELECT question_count,updated_at FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND actor_id = ? AND event_name = 'collection.play'"
            + " ORDER BY updated_at DESC LIMIT 1";
//    public static final String SELECT_CLASS_COLLECTION_QUESTION_COUNT = "SELECT question_count,updated_at FROM base_reports "
//            + "WHERE class_id = ? AND collection_id = ? AND actor_id = ? AND event_name = 'collection.play'"
//            + " ORDER BY updated_at DESC LIMIT 1";
    //Getting COLLECTION DATA (views, time_spent)
    public static final String SELECT_COLLECTION_AGG_DATA = "SELECT SUM(CASE WHEN (agg.event_name = 'collection.resource.play') THEN agg.time_spent ELSE 0 END) AS collectionTimeSpent, "
            + "SUM(CASE WHEN (agg.event_name = 'collection.play') THEN agg.views ELSE 0 END) AS collectionViews,"
            + "agg.collection_id, agg.completionStatus, 0 AS score, 0 AS reaction FROM "
            + "(SELECT collection_id,time_spent,session_id,views, event_name, "
            + "CASE  WHEN (FIRST_VALUE(event_type) OVER (PARTITION BY collection_id ORDER BY updated_at desc) = 'stop') THEN 'completed' ELSE 'in-progress' END AS completionStatus "
            + "FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND actor_id = ? ) AS agg "
            + "GROUP BY agg.collection_id,agg.completionStatus";
    //Getting COLLECTION DATA (score)
    public static final String SELECT_COLLECTION_AGG_SCORE = "SELECT SUM(agg.score) AS score FROM "
            + "(SELECT DISTINCT ON (resource_id) collection_id, "
            + "FIRST_VALUE(score) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS score "
            + "FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND actor_id = ? AND "
            + "event_name = 'collection.resource.play' AND resource_type = 'question' AND resource_attempt_status <> 'skipped' ) AS agg "
            + "GROUP BY agg.collection_id";
    //Getting COLLECTION DATA (reaction)
    public static final String SELECT_COLLECTION_AGG_REACTION = "SELECT ROUND(AVG(agg.reaction)) AS reaction "
            + "FROM (SELECT DISTINCT ON (resource_id) collection_id,  "
            + "FIRST_VALUE(reaction) OVER (PARTITION BY resource_id ORDER BY updated_at desc) "
            + "AS reaction FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? "
            + "AND actor_id = ? AND event_name = 'collection.resource.play' AND reaction <> 0) AS agg GROUP BY agg.collection_id";
    //Getting RESOURCE DATA (views, time_spent)
    public static final String SELECT_COLLECTION_RESOURCE_AGG_DATA = "SELECT collection_id, resource_id ,resource_type,question_type, SUM(views) AS resourceViews, "
            + "SUM(time_spent) AS resourceTimeSpent, 0 as reaction, 0 as score, '[]' AS answer_object "
            + "FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? "
            + "AND actor_id = ? AND event_name = 'collection.resource.play' GROUP BY collection_id, resource_id,resource_type,question_type";
  //Getting RESOURCE DATA (score)
    public static final String SELECT_COLLECTION_QUESTION_AGG_SCORE = "SELECT DISTINCT ON (resource_id) "
            + "FIRST_VALUE(score) OVER (PARTITION BY resource_id "
            + "ORDER BY updated_at desc) AS score,FIRST_VALUE(resource_attempt_status) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS attemptStatus, FIRST_VALUE(answer_object) OVER (PARTITION BY resource_id ORDER BY updated_at desc) AS answer_object "
            + "FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND resource_id = ?"
            + "AND actor_id = ? AND event_name = 'collection.resource.play' AND resource_type = 'question' AND resource_attempt_status <> 'skipped'";
  //Getting RESOURCE DATA (reaction)
    public static final String SELECT_COLLECTION_RESOURCE_AGG_REACTION = "SELECT DISTINCT ON (resource_id) "
            + "FIRST_VALUE(reaction) OVER (PARTITION BY resource_id "
            + "ORDER BY updated_at desc) AS reaction "
            + "FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND resource_id = ? "
            + "AND actor_id = ? AND event_name = 'collection.resource.play' AND reaction <> 0";

    
    //*****************************************************************************************************************************
    
    public static final String UUID_TYPE = "uuid";

}
