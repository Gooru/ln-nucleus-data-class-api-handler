package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;


/**
 * @author mukul@gooru
 * Modified by daniel
 */

@Table("base_reports")
public class AJEntityBaseReports extends Model {

    
	public static final String ID = "id";
	public static final String SEQUENCE_ID = "sequence_id";
	public static final String EVENTNAME = "event_name";
	
	public static final String EVENTTYPE = "event_type";
	//actor_id is userId or gooruuid
	public static final String GOORUUID = "actor_id";    
    
	public static final String CLASS_GOORU_OID = "class_id";
	public static final String COURSE_GOORU_OID = "course_id";
	public static final String UNIT_GOORU_OID = "unit_id";
	public static final String LESSON_GOORU_OID = "lesson_id";
    public static final String COLLECTION_OID = "collection_id";

    public static final String QUESTION_COUNT = "question_count";
    public static final String SESSION_ID = "session_id";
    public static final String COLLECTION_TYPE = "collection_type";
    public static final String RESOURCE_TYPE = "resource_type";
    public static final String QUESTION_TYPE = "question_type";
    public static final String ANSWER_OBECT = "answer_object";
    public static final String RESOURCE_ID = "resource_id";
    
    public static final String RESOURCE_VIEWS = "resourceViews";
    public static final String COLLECTION_VIEWS = "collectionViews";
    public static final String RESOURCE_time_spent = "resourcetime_spent";
    public static final String COLLECTION_time_spent = "collectiontime_spent";
    
    //Mukul - This has become redundant. Need to delete from Schema finally
    public static final String VIEWS = "views";
    public static final String REACTION = "reaction";
    
    //Mukul - enum (correct / incorrect / skipped / unevaluated)​
    public static final String RESOURCE_ATTEMPT_STATUS = "resource_attempt_status";    
    public static final String SCORE = "score";
    public static final String CREATE_TIMESTAMP = "created_timestamp";
    public static final String UPDATE_TIMESTAMP = "updated_timestamp";   
    
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
    public static final String ATTR_COLLECTION_TYPE = "collectionType";

    public static final String NA = "NA";
    public static final String AND = "AND";
    public static final String SPACE = " ";
    public static final String UNIT_ID = "unit_id = ? ";
    public static final String LESSON_ID = "lesson_id = ?";
    public static final String CLASS_ID = "class_id = ? ";
    
    
    
    public static final String SELECT_BASEREPORT_MAX_SEQUENCE_ID =
            "SELECT max(sequence_id) FROM base_reports";
    
    //String Constants and Queries for STUDENT PERFORMANCE REPORTS IN COURSE
    public static final String SELECT_DISTINCT_UNIT_ID_FOR_COURSE_ID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(unit_id) FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND collection_type =? AND actor_id = ?";

    public static final String SELECT_DISTINCT_USERID_FOR_COURSE_ID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(actor_id) FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND collection_type =?";
    
    public static final String SELECT_STUDENT_COURSE_PERF_FOR_ASSESSMENT =
              "SELECT SUM(agg.time_spent) as timeSpent, "
            + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.unit_id, 'completed' AS attemptStatus "
            + "FROM (SELECT time_spent, "
            + "reaction AS reaction, views AS attempts, unit_id FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND collection_type =? AND actor_id = ? AND unit_id = ANY(?::varchar[]) AND "
            + "event_name = ? AND event_type = 'stop') AS agg "
            + "GROUP BY agg.unit_id";
    public static final String SELECT_STUDENT_COURSE_PERF_FOR_COLLECTION =
            "SELECT SUM(agg.time_spent) as timeSpent, "
          + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.unit_id, 'completed' AS attemptStatus "
          + "FROM (SELECT time_spent, "
          + "reaction AS reaction, views AS attempts, unit_id FROM base_reports "
          + "WHERE class_id = ? AND course_id = ? AND collection_type =? AND actor_id = ? AND unit_id = ANY(?::varchar[]) AND "
          + "event_name = ? ) AS agg "
          + "GROUP BY agg.unit_id";
  
/*    public static final String SELECT_STUDENT_COURSE_PERF_FOR_COLLECTION =
            "SELECT SUM(collectiontime_spent) AS time_spent, SUM(collectionViews) AS views, SUM(reaction) AS reaction, unit_id FROM base_reports "
            + "WHERE unit_id = ANY(?::varchar[]) AND collection_type =? AND actor_id = ? GROUP BY unit_id";
    */
    public static final String GET_COMPLETED_COLLID_COUNT_FOREACH_UNIT_ID = 
    		"SELECT SUM(unitData.completion) AS completedCount, ROUND(AVG(scoreInPercentage)) scoreInPercentage FROM "
    		+ "(SELECT DISTINCT ON (collection_id) CASE  WHEN (event_type = 'stop') THEN 1 ELSE 0 END AS completion,"
    		+ "FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) AS scoreInPercentage,"
    		+ "unit_id FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND "
    		+ "collection_type =? AND actor_id = ? AND event_name = ? AND event_type = 'stop' ORDER BY collection_id, updated_timestamp DESC) "
    		+ "AS unitData GROUP BY unit_id;";
    
    public static final String GET_COMPLETED_COLL_COUNT_FOREACH_UNIT_ID = 
            "SELECT SUM(unitData.completion) AS completedCount, ROUND(AVG(scoreInPercentage)) scoreInPercentage FROM "
            + "(SELECT DISTINCT ON (collection_id) CASE  WHEN (event_type = 'stop') THEN 1 ELSE 0 END AS completion,"
            + "FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) AS scoreInPercentage,"
            + "unit_id FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND "
            + "collection_type =? AND actor_id = ? AND event_name = ?  ORDER BY collection_id, updated_timestamp DESC) "
            + "AS unitData GROUP BY unit_id;";
        
    
    //*************************************************************************************************************************
    //String Constants and Queries for STUDENT PERFORMANCE REPORTS IN UNIT    
    public static final String SELECT_DISTINCT_LESSON_ID_FOR_UNIT_ID_FITLERBY_COLLTYPE =
            "SELECT DISTINCT(lesson_id) FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND collection_type = ? AND actor_id = ?";
    
    public static final String SELECT_DISTINCT_USERID_FOR_UNIT_ID_FITLERBY_COLLTYPE =
            "SELECT DISTINCT(actor_id) FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND collection_type = ?";
    
    public static final String SELECT_DISTINCT_COLLID_FOR_LESSON_ID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(collection_id) FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_type =? AND actor_id = ?";

    public static final String SELECT_DISTINCT_USERID_FOR_LESSON_ID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(actor_id) FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_type =?";

    public static final String SELECT_DISTINCT_USERID_FOR_COLLECTION_ID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(actor_id) FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND collection_type =?";

    public static final String SELECT_STUDENT_EACH_UNIT_PERF_FOR_ASSESSMENT =
            "SELECT SUM(collectiontime_spent) AS time_spent, SUM(score) AS scoreInPercentage, SUM(reaction) AS reaction, lesson_id FROM base_reports "
            + "WHERE lesson_id = ? AND actor_id = ? GROUP BY lesson_id";
    
    public static final String SELECT_STUDENT_EACH_UNIT_PERF_FOR_COLLECTION =
            "SELECT SUM(collectiontime_spent) AS time_spent, SUM(collectionViews) AS views, SUM(reaction) AS reaction, lesson_id FROM base_reports "
            + "WHERE lesson_id = ? AND actor_id = ? GROUP BY lesson_id";
    
/*    public static final String GET_COMPLETED_COLLID_COUNT_FOREACH_lesson_id = 
    		"SELECT COUNT(collection_id) as completedCount, lesson_id from base_reports "
    		+ "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_type = ? "
    		+ "AND actor_id = ? AND event_name = ? AND event_type = ? GROUP BY lesson_id";
*/
    public static final String GET_COMPLETED_COLLID_COUNT_FOREACH_LESSON_ID = 
            "SELECT SUM(lessonData.completion) AS completedCount,ROUND(AVG(scoreInPercentage)) scoreInPercentage FROM "
            + "(SELECT DISTINCT ON (collection_id) CASE  WHEN (event_type = 'stop') THEN 1 ELSE 0 END AS completion, "
            + "lesson_id, FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) AS scoreInPercentage "
            + "FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? "
            + " AND collection_type =? AND actor_id = ? AND event_name = ? AND event_type = 'stop' ORDER BY collection_id, updated_timestamp DESC) "
            + "AS lessonData GROUP BY lesson_id;";
    public static final String GET_COMPLETED_COLL_COUNT_FOREACH_LESSON_ID = 
            "SELECT SUM(lessonData.completion) AS completedCount,ROUND(AVG(scoreInPercentage)) scoreInPercentage FROM "
            + "(SELECT DISTINCT ON (collection_id) CASE  WHEN (event_type = 'stop') THEN 1 ELSE 0 END AS completion, "
            + "lesson_id, FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) AS scoreInPercentage "
            + "FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? "
            + " AND collection_type =? AND actor_id = ? AND event_name = ? ORDER BY collection_id, updated_timestamp DESC) "
            + "AS lessonData GROUP BY lesson_id;";  
    public static final String SELECT_STUDENT_UNIT_PERF_FOR_ASSESSMENT =
            "SELECT SUM(agg.time_spent) AS timeSpent, "
          + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.lesson_id AS lessonId, 'completed' AS attemptStatus "
          + "FROM (SELECT time_spent , "
          + "reaction AS reaction, views AS attempts, lesson_id FROM base_reports "
          + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND collection_type =? AND actor_id = ? AND lesson_id = ANY(?::varchar[]) AND "
          + "event_name = ? AND event_type = 'stop') AS agg "
          + "GROUP BY agg.lesson_id";
  
    public static final String SELECT_STUDENT_UNIT_PERF_FOR_COLLECTION =
            "SELECT SUM(agg.time_spent) timeSpent, "
          + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.lessonId, 'completed' AS attemptStatus "
          + "FROM (SELECT time_spent, "
          + "reaction AS reaction, views AS attempts, lesson_id FROM base_reports "
          + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND collection_type =? AND actor_id = ? AND lesson_id = ANY(?::varchar[]) AND "
          + "event_name = ?) AS agg "
          + "GROUP BY agg.lesson_id";
    
    //*************************************************************************************************************************
    //String Constants and Queries for STUDENT PERFORMANCE REPORTS IN LESSON
    public static final String SELECT_STUDENT_LESSON_PERF =
            "SELECT coalesce(SUM(collectiontime_spent),0) AS time_spent, SUM(score) AS scoreInPercentage, SUM(reaction) AS reaction, "
            + "coalesce(SUM(collectionViews),0) AS attempts, collection_id, case  when (event_type = 'stop') then 'completed' else 'in-progress' end as attemptStatus FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND "
            + "collection_type = ? AND actor_id = ? GROUP BY collection_id,event_type";
    
   /* public static final String SELECT_STUDENT_LESSON_PERF_FOR_ASSESSMENT =
                        "SELECT SUM(collectiontime_spent) AS time_spent, SUM(score) AS scoreInPercentage, SUM(reaction) AS reaction, "
                       + "SUM(collectionViews) AS attempts, collection_id FROM base_reports "
                        + "WHERE collection_id = ANY(?::varchar[]) AND actor_id = ? GROUP BY collection_id";*/
    
    public static final String SELECT_STUDENT_LESSON_PERF_FOR_ASSESSMENT =
            "SELECT SUM(agg.time_spent) as timeSpent, ROUND(AVG(agg.scoreInPercentage)) scoreInPercentage, "
          + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.collection_id as collectionId, 'completed' AS attemptStatus "
          + "FROM (SELECT time_spent, FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) "
          + "AS scoreInPercentage, reaction AS reaction, views AS attempts, collection_id FROM base_reports "
          + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ANY(?::varchar[]) AND actor_id = ? AND "
          + "event_name = ? AND event_type = 'stop') AS agg "
          + "GROUP BY agg.collection_id";
    
    public static final String SELECT_STUDENT_LESSON_PERF_FOR_COLLECTION =
            "SELECT SUM(agg.time_spent) as timeSpent, ROUND(AVG(agg.scoreInPercentage)) scoreInPercentage, "
          + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.collection_id AS collectionId, 'completed' AS attemptStatus "
          + "FROM (SELECT time_spent, FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) "
          + "AS scoreInPercentage, reaction AS reaction, views AS attempts, collection_id FROM base_reports "
          + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ANY(?::varchar[]) AND actor_id = ? AND "
          + "event_name = ? ) AS agg "
          + "GROUP BY agg.collection_id";
   /* public static final String SELECT_STUDENT_LESSON_PERF_FOR_COLLECTION =
            "SELECT SUM(collectiontime_spent) AS time_spent, SUM(reaction) AS reaction, "
            + "SUM(collectionViews) AS views, collection_id FROM base_reports "
            + "WHERE collection_id = ANY(?::varchar[]) AND actor_id = ? GROUP BY collection_id";*/
    
    public static final String GET_COMPLETED_COLLID_COUNT = 
    		"SELECT COUNT(collection_id) as completedCount, collection_id as collectionId from base_reports "
    		+ "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND "
    		+ "collection_type = ? AND actor_id = ? AND event_name = ? AND event_type = ? GROUP BY collection_id";
    
    //*************************************************************************************************************************
    //STUDENT PERFORMANCE in Assessment
    public static final String GET_LATEST_COMPLETED_SESSION_ID = "SELECT session_id FROM base_reports WHERE"
            +" class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND actor_id = ? AND"
            +" event_name = 'collection.play' AND event_type = 'stop'"
            +" ORDER BY created_timestamp ASC";
    
    //*************************************************************************************************************************
    //String Constants and Queries for STUDENT PERFORMANCE REPORTS IN ASSESSMENTS    
    public static final String SELECT_ASSESSMENT_FOREACH_COLLID_AND_SESSION_ID =
            "select distinct on (collection_id) FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) AS score,"
            + "collection_id,FIRST_VALUE(reaction) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) AS reaction,"
            + "FIRST_VALUE(time_spent) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) as collectionTimeSpent,"
            + "updated_timestamp,session_id,collection_type,FIRST_VALUE(views) OVER (PARTITION BY collection_id ORDER BY updated_timestamp asc) AS collectionViews "
            + "from base_reports WHERE session_id = ? AND event_name = ? ";
    
    
    public static final String SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSION_ID =
            "select  distinct on (resource_id) FIRST_VALUE(score * 100) OVER (PARTITION BY resource_id ORDER BY updated_timestamp desc) AS score,"
            + "resource_id,FIRST_VALUE(reaction) OVER (PARTITION BY resource_id ORDER BY updated_timestamp desc) AS reaction,"
            + "FIRST_VALUE(time_spent) OVER (PARTITION BY resource_id ORDER BY updated_timestamp desc) as resourceTimeSpent,"
            + "updated_timestamp,session_id,collection_type,"
            + "FIRST_VALUE(views) OVER (PARTITION BY resource_id ORDER BY updated_timestamp asc) AS resourceViews, "
            + "resource_type,question_type,"
            + "FIRST_VALUE(answer_object) OVER (PARTITION BY resource_id ORDER BY updated_timestamp desc) as answer_object "
            + "from base_reports WHERE session_id = ? AND event_name = ? ";
    
    public static final String SELECT_COLLECTION_FOREACH_COLLID_AND_SESSION_ID =
            "SELECT distinct on (collection_id) score,collection_id,reaction,time_spent AS collectiontime_spent,created_timestamp,session_id,collection_type,views AS collectionviews from base_reports"
            + " WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND actor_id = ? AND event_name = ?"
            ;
    
    public static final String SELECT_COLLECTION_RESOURCE_FOREACH_COLLID_AND_SESSION_ID =
            "SELECT distinct on (resource_id) score,collection_id,reaction,time_spent AS resourcetime_spent,created_timestamp,session_id,collection_type,views AS resourceviews,resource_type,question_type,answer_object as answer_object from base_reports"
            + " WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND actor_id = ? AND event_name = ?"
            ; 
  //*************************************************************************************************************************
    //Collection Summary report Queries
    //Getting collection question count
    public static final String SELECT_COLLECTION_QUESTION_COUNT = "SELECT question_count,updated_timestamp FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND actor_id = ? AND event_name = 'collection.play'"
            + " ORDER BY updated_timestamp DESC LIMIT 1";
    //Getting COLLECTION DATA (views, time_spent)
    public static final String SELECT_COLLECTION_AGG_DATA = "SELECT SUM(agg.time_spent) AS collectionTimeSpent, SUM(agg.views) AS collectionViews,"
            + "agg.collection_id, agg.completionStatus, 0 AS score, 0 AS reaction FROM "
            + "(SELECT collection_id,time_spent,session_id,views,"
            + "CASE  WHEN (FIRST_VALUE(event_type) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) = 'stop') THEN 'completed' ELSE 'in-progress' END AS completionStatus "
            + "FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND actor_id = ? AND event_name = 'collection.play' ) AS agg "
            + "GROUP BY agg.collection_id,agg.completionStatus";
    //Getting COLLECTION DATA (score)
    public static final String SELECT_COLLECTION_AGG_SCORE = "SELECT SUM(agg.score) AS score FROM "
            + "(SELECT DISTINCT ON (resource_id) collection_id, "
            + "FIRST_VALUE(score) OVER (PARTITION BY resource_id ORDER BY updated_timestamp desc) AS score "
            + "FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND actor_id = ? AND "
            + "event_name = 'collection.resource.play' AND resource_type = 'question' AND resource_attempt_status <> 'skipped' ) AS agg "
            + "GROUP BY agg.collection_id";
    //Getting COLLECTION DATA (reaction)
    public static final String SELECT_COLLECTION_AGG_REACTION = "SELECT ROUND(AVG(agg.reaction)) AS reaction "
            + "FROM (SELECT DISTINCT ON (resource_id) collection_id,  "
            + "FIRST_VALUE(reaction) OVER (PARTITION BY resource_id ORDER BY updated_timestamp desc) "
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
            + "ORDER BY updated_timestamp desc) AS score,FIRST_VALUE(resource_attempt_status) OVER (PARTITION BY resource_id ORDER BY updated_timestamp desc) AS attemptStatus, FIRST_VALUE(answer_object) OVER (PARTITION BY resource_id ORDER BY updated_timestamp desc) AS answer_object "
            + "FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND resource_id = ?"
            + "AND actor_id = ? AND event_name = 'collection.resource.play' AND resource_type = 'question' AND resource_attempt_status <> 'skipped'";
  //Getting RESOURCE DATA (reaction)
    public static final String SELECT_COLLECTION_RESOURCE_AGG_REACTION = "SELECT DISTINCT ON (resource_id) "
            + "FIRST_VALUE(reaction) OVER (PARTITION BY resource_id "
            + "ORDER BY updated_timestamp desc) AS reaction "
            + "FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND resource_id = ? "
            + "AND actor_id = ? AND event_name = 'collection.resource.play' AND reaction <> 0";
  
  //*************************************************************************************************************************
    //Collection Summary report Queries for context of outside class
    //Getting collection question count
  //Getting collection question count in the context of outside class
    public static final String SELECT_COLLECTION_QUESTION_COUNT_ = "SELECT question_count,updated_timestamp FROM base_reports "
            + "WHERE class_id IS NULL AND course_id IS NULL AND unit_id IS NULL AND lesson_id IS NULL AND collection_id = ? AND actor_id = ? AND event_name = 'collection.play'"
            + " ORDER BY updated_timestamp DESC LIMIT 1";
    
    //Getting COLLECTION DATA (views, time_spent)
    public static final String SELECT_COLLECTION_AGG_DATA_ = "SELECT SUM(agg.time_spent) AS collectionTimeSpent, SUM(agg.views) AS collectionViews,"
            + "agg.collection_id, agg.completionStatus, 0 AS score, 0 AS reaction FROM "
            + "(SELECT collection_id,time_spent,session_id,views,"
            + "CASE  WHEN (FIRST_VALUE(event_type) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) = 'stop') THEN 'completed' ELSE 'in-progress' END AS completionStatus "
            + "FROM base_reports WHERE class_id IS NULL AND course_id IS NULL AND unit_id IS NULL AND lesson_id IS NULL AND collection_id = ? AND actor_id = ? AND event_name = 'collection.play' ) AS agg "
            + "GROUP BY agg.collection_id,agg.completionStatus";
    //Getting COLLECTION DATA (score)
    public static final String SELECT_COLLECTION_AGG_SCORE_ = "SELECT SUM(agg.score) AS score FROM "
            + "(SELECT DISTINCT ON (resource_id) collection_id, "
            + "FIRST_VALUE(score) OVER (PARTITION BY resource_id ORDER BY updated_timestamp desc) AS score "
            + "FROM base_reports WHERE class_id IS NULL AND course_id IS NULL AND unit_id IS NULL AND lesson_id IS NULL AND collection_id = ? AND actor_id = ? AND "
            + "event_name = 'collection.resource.play' AND resource_type = 'question' AND resource_attempt_status <> 'skipped' ) AS agg "
            + "GROUP BY agg.collection_id";
    //Getting COLLECTION DATA (reaction)
    public static final String SELECT_COLLECTION_AGG_REACTION_ = "SELECT ROUND(AVG(agg.reaction)) AS reaction "
            + "FROM (SELECT DISTINCT ON (resource_id) collection_id,  "
            + "FIRST_VALUE(reaction) OVER (PARTITION BY resource_id ORDER BY updated_timestamp desc) "
            + "AS reaction FROM base_reports "
            + "WHERE class_id IS NULL AND course_id IS NULL AND unit_id IS NULL AND lesson_id IS NULL AND collection_id = ? "
            + "AND actor_id = ? AND event_name = 'collection.resource.play' AND reaction <> 0) AS agg GROUP BY agg.collection_id";
    //Getting RESOURCE DATA (views, time_spent)
    public static final String SELECT_COLLECTION_RESOURCE_AGG_DATA_ = "SELECT collection_id, resource_id ,resource_type,question_type, SUM(views) AS resourceViews, "
            + "SUM(time_spent) AS resourceTimeSpent, 0 as reaction, 0 as score, '[]' AS answer_object "
            + "FROM base_reports WHERE class_id IS NULL AND course_id IS NULL AND unit_id IS NULL AND lesson_id IS NULL AND collection_id = ? "
            + "AND actor_id = ? AND event_name = 'collection.resource.play' GROUP BY collection_id, resource_id,resource_type,question_type";
  //Getting RESOURCE DATA (score)
    public static final String SELECT_COLLECTION_QUESTION_AGG_SCORE_ = "SELECT DISTINCT ON (resource_id) "
            + "FIRST_VALUE(score) OVER (PARTITION BY resource_id "
            + "ORDER BY updated_timestamp desc) AS score,FIRST_VALUE(resource_attempt_status) OVER (PARTITION BY resource_id ORDER BY updated_timestamp desc) AS attemptStatus, FIRST_VALUE(answer_object) OVER (PARTITION BY resource_id ORDER BY updated_timestamp desc) AS answer_object "
            + "FROM base_reports WHERE class_id IS NULL AND course_id IS NULL AND unit_id IS NULL AND lesson_id IS NULL AND collection_id = ? AND resource_id = ?"
            + "AND actor_id = ? AND event_name = 'collection.resource.play' AND resource_type = 'question' AND resource_attempt_status <> 'skipped'";
  //Getting RESOURCE DATA (reaction)
    public static final String SELECT_COLLECTION_RESOURCE_AGG_REACTION_ = "SELECT DISTINCT ON (resource_id) "
            + "FIRST_VALUE(reaction) OVER (PARTITION BY resource_id "
            + "ORDER BY updated_timestamp desc) AS reaction "
            + "FROM base_reports WHERE class_id IS NULL AND course_id IS NULL AND unit_id IS NULL AND lesson_id IS NULL AND collection_id = ? AND resource_id = ? "
            + "AND actor_id = ? AND event_name = 'collection.resource.play' AND reaction <> 0";
      
  //*************************************************************************************************************************
      // GET CURRENT STUDENT LOCATITON
    public static final String GET_STUDENT_LOCATION = 
    		"select class_id, course_id, unit_id, lesson_id, collection_id,collection_type, created_timestamp, updated_timestamp from base_reports "
    		+ " WHERE class_id = ? AND actor_id = ? ORDER BY updated_timestamp DESC LIMIT 1";
    
 // GET STUDENT's PEERS IN COURSE
    public static final String GET_PEERS_COUNT_IN_COURSE = 
        "SELECT count(aId) AS peerCount, unit_id FROM "
        + "(SELECT DISTINCT ON (actor_id) collection_id, course_id, lesson_id, unit_id as unit_id, actor_id as aId, "
        + "updated_timestamp FROM base_reports where class_id = ? AND actor_id <> ? "
        + "ORDER BY actor_id, updated_timestamp DESC) AS DS "
        + "WHERE DS.course_id = ?  GROUP BY unit_id;";

    // GET STUDENT's PEERS IN UNIT
    public static final String GET_PEERS_COUNT_IN_UNIT= 
        "SELECT count(aId) AS peerCount, lesson_id FROM "
        + "(SELECT DISTINCT ON (actor_id) collection_id, course_id, lesson_id, unit_id as unit_id, actor_id as aId, "
        + "updated_timestamp FROM base_reports where class_id = ? AND actor_id <> ? "
        + "ORDER BY actor_id, updated_timestamp DESC) AS DS "
        + "WHERE  DS.course_id = ? AND DS.unit_id = ?  GROUP BY lesson_id";

  //GET STUDENT's PEERS IN LESSON
    public static final String GET_PEERS_IN_LESSON = "SELECT collection_id, actor_id, collection_type, updated_timestamp "
            + "FROM (SELECT DISTINCT ON (actor_id) collection_id, course_id, lesson_id, unit_id as unit_id, actor_id , collection_type, updated_timestamp "
            + "FROM base_reports where class_id = ? AND actor_id <> ? ORDER BY actor_id, updated_timestamp DESC) AS DS "
            + "WHERE DS.course_id = ? AND DS.unit_id = ?  AND lesson_id = ?";

    public static final String GET_DISTINCT_USERS_FOR_COLLECTION_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(actor_id) AS Users, collection_id FROM base_reports "
            + "WHERE collection_id = ? AND collection_type =?";
    
    public static final String GET_USERS_LATEST_TIMESTAMP = 
    		"SELECT updated_timestamp, actor_id from base_reports "
    		+ " WHERE lesson_id = ? AND collection_id = ? AND collection_type =? AND actor_id = ? ORDER BY updated_timestamp DESC LIMIT 1";

  //*************************************************************************************************************************    
    //GET SESSION STATUS    
    /**public static final String GET_SESSION_STATUS =  "SELECT count(*) from base_reports WHERE session_id = ? "
    		+ " AND collection_id = ? AND event_type = ? AND event_name = ? "; **/
    
    public static final String GET_SESSION_STATUS =  "SELECT event_name, event_type, created_timestamp from base_reports WHERE session_id = ? "
	+ " AND collection_id = ? AND event_name = ? ";
    
    //GET USER ALL SESSIONS FROM ASSESSMENT    
    public static final String GET_USER_SESSIONS_FOR_COLLID =  "SELECT DISTINCT s.session_id,s.updated_timestamp FROM "
            + "(SELECT FIRST_VALUE(updated_timestamp) OVER (PARTITION BY session_id ORDER BY updated_timestamp DESC) AS updated_timestamp, session_id "
            + "FROM base_reports WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_id = ? AND collection_type = ? AND actor_id = ?"
            + " ORDER  BY updated_timestamp DESC) as s ORDER BY s.updated_timestamp DESC;";
    
    //GET USER ALL SESSIONS FROM ASSESSMENT  OUT OF CLASS  
    public static final String GET_USER_SESSIONS_FOR_COLLID_ =  "SELECT DISTINCT s.session_id,s.updated_timestamp "
            + "FROM (SELECT FIRST_VALUE(updated_timestamp) OVER (PARTITION BY session_id ORDER BY updated_timestamp DESC) AS updated_timestamp, session_id "
            + "FROM base_reports WHERE class_id IS NULL  AND course_id IS NULL  AND unit_id IS NULL  AND lesson_id IS NULL  AND collection_id = ? "
            + " AND collection_type = ? ORDER  BY updated_timestamp DESC) as s ORDER BY s.updated_timestamp DESC;";
    
  //*************************************************************************************************************************
    // TEACHER REPORTS
    //String Constants and Queries for ALL STUDENT PERFORMANCE REPORTS IN COURSE

    public static final String SELECT_DISTINCT_unit_id_FOR_course_id_FILTERBY_COLLTYPE_FORALLUSERS =
            "SELECT DISTINCT(unit_id) FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND collection_type =?";

    public static final String SELECT_STUDENT_COURSE_PERF_FOR_ASSESSMENT_FORALLUSERS =
            "SELECT SUM(collectiontime_spent) AS time_spent, SUM(score) AS scoreInPercentage, SUM(collectionViews) AS attempts, "
            + "SUM(reaction) AS reaction, unit_id FROM base_reports "
            + "WHERE unit_id = ANY(?::varchar[]) AND collection_type =? GROUP BY unit_id";
    
    public static final String SELECT_STUDENT_COURSE_PERF_FOR_COLLECTION_FORALLUSERS =
            "SELECT SUM(collectiontime_spent) AS time_spent, SUM(collectionViews) AS views, SUM(reaction) AS reaction, unit_id FROM base_reports "
            + "WHERE unit_id = ANY(?::varchar[]) AND collection_type =? GROUP BY unit_id";
    
    public static final String GET_COMPLETED_COLLID_COUNT_FOREACH_unit_id_FORALLUSERS = 
    		"SELECT COUNT(collection_id) as completedCount, unit_id from base_reports "
    		+ "WHERE class_id = ? AND course_id = ? AND collection_type =? AND event_name = ? AND event_type = ? GROUP BY unit_id";
    
    //*************************************************************************************************************************
    //String Constants and Queries for ALL STUDENT PERFORMANCE REPORTS IN UNIT
    
    public static final String SELECT_DISTINCT_lesson_id_FOR_unit_id_FITLERBY_COLLTYPE_FORALLUSERS =
            "SELECT DISTINCT(lesson_id) FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND collection_type = ?";
    
    public static final String SELECT_DISTINCT_COLLID_FOR_lesson_id_FILTERBY_COLLTYPE_FORALLUSERS =
            "SELECT DISTINCT(collection_id) FROM base_reports "
            + "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_type =?";

    public static final String SELECT_STUDENT_EACH_UNIT_PERF_FOR_ASSESSMENT_FORALLUSERS =
            "SELECT SUM(collectiontime_spent) AS time_spent, SUM(score) AS scoreInPercentage, SUM(reaction) AS reaction, lesson_id FROM base_reports "
            + "WHERE lesson_id = ? GROUP BY lesson_id";
    
    public static final String SELECT_STUDENT_EACH_UNIT_PERF_FOR_COLLECTION_FORALLUSERS =
            "SELECT SUM(collectiontime_spent) AS time_spent, SUM(collectionViews) AS views, SUM(reaction) AS reaction, lesson_id FROM base_reports "
            + "WHERE lesson_id = ? GROUP BY lesson_id";
    
    public static final String GET_COMPLETED_COLLID_COUNT_FOREACH_lesson_id_FORALLUSERS = 
    		"SELECT COUNT(collection_id) as completedCount, lesson_id from base_reports "
    		+ "WHERE class_id = ? AND course_id = ? AND unit_id = ? AND lesson_id = ? AND collection_type = ? "
    		+ "AND event_name = ? AND event_type = ? GROUP BY lesson_id";
    
    public static final String SELECT_STUDENT_LESSON_PERF_FOR_ASSESSMENT_FORALLUSERS =
            "SELECT SUM(collectiontime_spent) AS time_spent, SUM(score) AS scoreInPercentage, SUM(reaction) AS reaction, "
            + "SUM(collectionViews) AS attempts, collection_id FROM base_reports "
            + "WHERE collection_id = ANY(?::varchar[]) GROUP BY collection_id";
    
    public static final String SELECT_STUDENT_LESSON_PERF_FOR_COLLECTION_FORALLUSERS =
            "SELECT SUM(collectiontime_spent) AS time_spent, SUM(reaction) AS reaction, "
            + "SUM(collectionViews) AS views, collection_id FROM base_reports "
            + "WHERE collection_id = ANY(?::varchar[]) GROUP BY collection_id";
    
    public static final String SELECT_CLASS_USER_BY_SESSION_ID = "SELECT class_id,actor_id FROM base_reports WHERE session_id = ? LIMIT 1";

    //*************************************************************************************************************************
    //Student all classes performance
    
    //CLASS DATA FOR A USER (attempts, time_spent)
    public static final String SELECT_STUDENT_ALL_CLASS_DATA = "SELECT SUM(time_spent) AS timeSpent,  SUM(views) AS attempts, class_id, course_id "
            + "FROM base_reports WHERE class_id = ANY(?::varchar[]) AND actor_id = ? "
            + "AND event_name = 'collection.play' GROUP BY class_id,course_id";

    //CLASS DATA FOR ALL USER(attempts, time_spent)
    public static final String SELECT_ALL_STUDENT_ALL_CLASS_DATA = "SELECT SUM(time_spent) AS timeSpent,  SUM(views) AS attempts, class_id, course_id "
            + "FROM base_reports WHERE class_id = ANY(?::varchar[]) "
            + "AND event_name = 'collection.play' GROUP BY class_id,course_id";
    
    //CLASS DATA FOR A USER(score, completion)
    public static final String SELECT_STUDENT_ALL_CLASS_COMPLETION_SCORE = "SELECT class_id, SUM(classData.completion) AS completedCount, ROUND(AVG(scoreInPercentage)) AS scoreInPercentage "
            + "FROM (SELECT DISTINCT ON (collection_id) CASE  WHEN (event_type = 'stop') THEN 1 ELSE 0 END AS completion, "
            + "FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) AS scoreInPercentage, class_id "
            + "FROM base_reports WHERE class_id = ?  AND actor_id = ? "
            + "AND event_name = 'collection.play' AND event_type = 'stop' AND collection_type = 'assessment' "
            + "ORDER BY collection_id, updated_timestamp DESC) AS classData GROUP BY class_id";

    //CLASS DATA FOR ALL USER(score, completion)
    public static final String SELECT_ALL_STUDENT_ALL_CLASS_COMPLETION_SCORE = "SELECT class_id, SUM(classData.completion) AS completedCount, ROUND(AVG(scoreInPercentage)) AS scoreInPercentage "
            + "FROM (SELECT DISTINCT ON (collection_id) CASE  WHEN (event_type = 'stop') THEN 1 ELSE 0 END AS completion, "
            + "FIRST_VALUE(score) OVER (PARTITION BY collection_id,actor_id ORDER BY updated_timestamp desc) AS scoreInPercentage, class_id "
            + "FROM base_reports WHERE class_id = ? "
            + "AND event_name = 'collection.play' AND event_type = 'stop' AND collection_type = 'assessment' "
            + "ORDER BY collection_id, updated_timestamp DESC) AS classData GROUP BY class_id";

    //*************************************************************************************************************************    
    //Student Location in All Classes    
    public static final String GET_STUDENT_LOCATION_ALL_CLASSES = "select DISTINCT ON (class_id) class_id, course_id, unit_id, "
    		+ "lesson_id, collection_id, collection_type, session_id, updated_timestamp FROM base_reports WHERE actor_id = ? AND class_id = ANY(?::varchar[]) "
    		+ "ORDER BY class_id, updated_timestamp DESC";
    
    public static final String GET_COLLECTION_STATUS =  "SELECT event_name, event_type from base_reports WHERE session_id = ? "
    		+ " AND collection_id = ? AND event_name = ? AND event_type = ?";

    //*************************************************************************************************************************
    
    //Student Performance for All Assessments/Collections in a Course
    public static final String GET_LATEST_SCORE_FOR_ASSESSMENT = "SELECT DISTINCT ON (collection_id) "
    		+ "FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) AS scoreInPercentage, "
    		+ "collection_id from base_reports WHERE collection_id = ? AND collection_type = ? "
    		+ "AND actor_id = ? AND event_name = ? AND event_type = ?";
    
    public static final String GET_TOTAL_TIME_SPENT_ATTEMPTS_FOR_ASSESSMENT = "SELECT SUM(time_spent) AS timeSpent, "
    		+ "SUM(views) AS attempts, collection_id FROM base_reports WHERE collection_id = ? AND collection_type = ? "
    		+ "AND actor_id = ? AND event_name = ? AND event_type = ? GROUP BY collection_id";
        
    public static final String GET_PERFORMANCE_FOR_COLLECTION = "SELECT SUM(time_spent) AS timeSpent, "
    		+ "SUM(views) AS views, collection_id FROM base_reports WHERE collection_id = ? AND collection_type = ? AND actor_id = ? "
    		+ "AND event_name = ? GROUP BY collection_id";
    
    public static final String GET_PERFORMANCE_FOR_CLASS_COLLECTION = "SELECT SUM(time_spent) AS timeSpent, "
    		+ "SUM(views) AS views, collection_id FROM base_reports WHERE class_id = ? AND collection_id = ? AND collection_type = ? AND actor_id = ? "
    		+ "AND event_name = ? GROUP BY collection_id";

    public static final String GET_DISTINCT_COLLECTIONS = "SELECT distinct(collection_id) from base_reports where "
    		+ "actor_id = ? AND collection_type = ? AND course_id = ? ";
    
    public static final String GET_PERFORMANCE_FOR_CLASS_ASSESSMENTS =
            "SELECT SUM(agg.time_spent) timeSpent, ROUND(AVG(agg.scoreInPercentage)) scoreInPercentage, "
          + "SUM(agg.attempts) attempts, agg.collection_id "
          + "FROM (SELECT time_spent, FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) "
          + "AS scoreInPercentage, views AS attempts, collection_id FROM base_reports "
          + "WHERE class_id = ? AND collection_id = ANY(?::varchar[]) AND actor_id = ? AND "
          + "event_name = ? AND event_type = 'stop') AS agg "
          + "GROUP BY agg.collection_id";
    
    public static final String GET_PERFORMANCE_FOR_ASSESSMENTS =
            "SELECT SUM(agg.time_spent) timeSpent, ROUND(AVG(agg.scoreInPercentage)) scoreInPercentage, "
          + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.collection_id, 'completed' AS attemptStatus "
          + "FROM (SELECT time_spent, FIRST_VALUE(score) OVER (PARTITION BY collection_id ORDER BY updated_timestamp desc) "
          + "AS scoreInPercentage, reaction AS reaction, views AS attempts, collection_id FROM base_reports "
          + "WHERE collection_id = ANY(?::varchar[]) AND actor_id = ? AND "
          + "event_name = ? AND event_type = 'stop') AS agg "
          + "GROUP BY agg.collection_id";

    //*************************************************************************************************************************
    
    
    public static final String UUID_TYPE = "uuid";
   
}
