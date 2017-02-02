package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Table;


/**
 * @author mukul@gooru
 *
 */

@Table("BaseReports")
public class AJEntityBaseReports extends Model {

    
	public static final String ID = "id";
	public static final String SEQUENCE_ID = "sequence_id";
	public static final String EVENTNAME = "eventName";
	
	public static final String EVENTTYPE = "eventType";
	//actorId is userId or gooruuid
	public static final String GOORUUID = "actorId";    
    
	public static final String CLASS_GOORU_OID = "classId";
	public static final String COURSE_GOORU_OID = "courseId";
	public static final String UNIT_GOORU_OID = "unitId";
	public static final String LESSON_GOORU_OID = "lessonId";
    public static final String COLLECTION_OID = "collectionId";

    public static final String QUESTION_COUNT = "question_count";
    public static final String SESSION_ID = "sessionId";
    public static final String COLLECTION_TYPE = "collectionType";
    public static final String RESOURCE_TYPE = "resourceType";
    public static final String QUESTION_TYPE = "questionType";
    public static final String ANSWER_OBECT = "answerObject";
    public static final String RESOURCE_ID = "resourceId";
    
    public static final String RESOURCE_VIEWS = "resourceViews";
    public static final String COLLECTION_VIEWS = "collectionViews";
    public static final String RESOURCE_TIMESPENT = "resourceTimeSpent";
    public static final String COLLECTION_TIMESPENT = "collectionTimeSpent";
    
    //Mukul - This has become redundant. Need to delete from Schema finally
    public static final String VIEWS = "views";
    public static final String REACTION = "reaction";
    
    //Mukul - enum (correct / incorrect / skipped / unevaluated)​
    public static final String RESOURCE_ATTEMPT_STATUS = "resourceAttemptStatus";    
    public static final String SCORE = "score";
    public static final String CREATE_TIMESTAMP = "createTimestamp";
    public static final String UPDATE_TIMESTAMP = "updateTimestamp";   
    
    public static final String ATTR_TIMESPENT = "timeSpent";
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
    public static final String NA = "NA";
    
    
    public static final String SELECT_BASEREPORT_MAX_SEQUENCE_ID =
            "SELECT max(sequence_id) FROM BaseReports";
    
    //String Constants and Queries for STUDENT PERFORMANCE REPORTS IN COURSE
    //TODO: Currently Score is calculated as SUM FOR TESTING PURPOSE, but QuestionCount is NOW available
    // so percentScore can be calculated.
    public static final String SELECT_DISTINCT_UNITID_FOR_COURSEID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(unitId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND collectionType =? AND actorId = ?";

    public static final String SELECT_DISTINCT_USERID_FOR_COURSEID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(actorId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND collectionType =?";
    
    public static final String SELECT_STUDENT_COURSE_PERF_FOR_ASSESSMENT =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(score) AS scoreInPercentage, SUM(collectionViews) AS attempts, "
            + "SUM(reaction) AS reaction, unitId FROM BaseReports "
            + "WHERE unitId = ANY(?::varchar[]) AND collectionType =? AND actorId = ? GROUP BY unitId";
    
    public static final String SELECT_STUDENT_COURSE_PERF_FOR_COLLECTION =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(collectionViews) AS views, SUM(reaction) AS reaction, unitId FROM BaseReports "
            + "WHERE unitId = ANY(?::varchar[]) AND collectionType =? AND actorId = ? GROUP BY unitId";
    
    public static final String GET_COMPLETED_COLLID_COUNT_FOREACH_UNITID = 
    		"SELECT COUNT(collectionId) as completedCount, unitId from basereports "
    		+ "WHERE classId = ? AND courseId = ? AND collectionType =? AND actorId = ? AND eventName = ? AND eventType = ? GROUP BY unitId";
    
    //*************************************************************************************************************************
    //String Constants and Queries for STUDENT PERFORMANCE REPORTS IN UNIT    
    public static final String SELECT_DISTINCT_LESSONID_FOR_UNITID_FITLERBY_COLLTYPE =
            "SELECT DISTINCT(lessonId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND unitId = ? AND collectionType = ? AND actorId = ?";
    
    public static final String SELECT_DISTINCT_USERID_FOR_UNITID_FITLERBY_COLLTYPE =
            "SELECT DISTINCT(actorId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND unitId = ? AND collectionType = ?";
    
    public static final String SELECT_DISTINCT_COLLID_FOR_LESSONID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(collectionId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND collectionType =? AND actorId = ?--";

    public static final String SELECT_DISTINCT_USERID_FOR_LESSONID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(actorId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND collectionType =?";

    
    public static final String SELECT_STUDENT_EACH_UNIT_PERF_FOR_ASSESSMENT =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(score) AS scoreInPercentage, SUM(reaction) AS reaction, lessonId FROM BaseReports "
            + "WHERE lessonId = ? AND actorId = ? GROUP BY lessonId";
    
    public static final String SELECT_STUDENT_EACH_UNIT_PERF_FOR_COLLECTION =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(collectionViews) AS views, SUM(reaction) AS reaction, lessonId FROM BaseReports "
            + "WHERE lessonId = ? AND actorId = ? GROUP BY lessonId";
    
    public static final String GET_COMPLETED_COLLID_COUNT_FOREACH_LESSONID = 
    		"SELECT COUNT(collectionId) as completedCount, lessonId from basereports "
    		+ "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND collectionType = ? "
    		+ "AND actorId = ? AND eventName = ? AND eventType = ? GROUP BY lessonId";

    
    //*************************************************************************************************************************
    //String Constants and Queries for STUDENT PERFORMANCE REPORTS IN LESSON
    public static final String SELECT_STUDENT_LESSON_PERF_FOR_ASSESSMENT =
            "SELECT coalesce(SUM(collectionTimeSpent),0) AS timeSpent, SUM(score) AS scoreInPercentage, SUM(reaction) AS reaction, "
            + "coalesce(SUM(collectionViews),0) AS attempts, collectionId, case  when (eventtype = 'stop') then 'completed' else 'in-progress' end as attemptStatus FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND "
            + "collectiontype = ? AND actorId = ? GROUP BY collectionId,eventtype";
    
    public static final String SELECT_STUDENT_LESSON_PERF_FOR_COLLECTION =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(reaction) AS reaction, "
            + "SUM(collectionViews) AS views, collectionId FROM BaseReports "
            + "WHERE collectionId = ANY(?::varchar[]) AND actorId = ? GROUP BY collectionId";
    
    public static final String GET_COMPLETED_COLLID_COUNT = 
    		"SELECT COUNT(collectionId) as completedCount, collectionId from basereports "
    		+ "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND "
    		+ "collectionType = ? AND actorId = ? AND eventName = ? AND eventType = ? GROUP BY collectionId";


    //*************************************************************************************************************************
    //String Constants and Queries for STUDENT PERFORMANCE REPORTS IN ASSESSMENTS
    public static final String SELECT_ASSESSMENT_FOREACH_COLLID_AND_SESSIONID =
            "select score,collectionid,reaction,collectiontimespent,createtimestamp,sessionid,collectiontype,coalesce(collectionviews,0) AS collectionviews from basereports"
            + " WHERE sessionId = ? AND eventName = ?"
            + " AND eventtype = ?";
    
    public static final String SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSIONID =
            "select score,collectionid,reaction,resourcetimespent,createtimestamp,sessionid,collectiontype,coalesce(resourceviews,0) AS resourceviews,resourcetype,questiontype,answerobject as answerobject from basereports"
            + " WHERE sessionId = ? AND eventName = ?"
            + " AND eventtype = ?";
    
    public static final String SELECT_COLLECTION_FOREACH_COLLID_AND_SESSIONID =
            "SELECT distinct on (collectionid) score,collectionid,reaction,coalesce(collectiontimespent, 0) AS collectiontimespent,createtimestamp,sessionid,collectiontype,coalesce(collectionviews,0) AS collectionviews from basereports"
            + " WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? AND collectionid = ? AND actorid = ? AND eventName = ?"
            ;
    
    public static final String SELECT_COLLECTION_RESOURCE_FOREACH_COLLID_AND_SESSIONID =
            "SELECT distinct on (resourceid) score,collectionid,reaction,coalesce(resourcetimespent,0) AS resourcetimespent,createtimestamp,sessionid,collectiontype,coalesce(resourceviews,0) AS resourceviews,resourcetype,questiontype,answerobject as answerobject from basereports"
            + " WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? AND collectionid = ? AND actorid = ? AND eventName = ?"
            ; 
  //*************************************************************************************************************************
    // GET CURRENT STUDENT LOCATITON
    public static final String GET_STUDENT_LOCATION = 
    		"select classid, courseid, unitid, lessonId, collectionId, createTimestamp, updateTimestamp from basereports "
    		+ " WHERE classId = ? AND actorId = ? ORDER BY updateTimestamp DESC LIMIT 1";

    // GET STUDENT's PEERS IN COURSE
    public static final String GET_STUDENT_PEERS_IN_COURSE = 
    		"select count(DISTINCT(actorID)) AS peerCount, unitId from BaseReports where classId = ? AND courseId = ? GROUP BY unitID";
    
    // GET STUDENT's PEERS IN UNIT
    public static final String GET_STUDENT_PEERS_IN_UNIT = 
    		"select count(DISTINCT(actorID)) AS peerCount, lessonId from BaseReports where classId = ? AND courseId = ? AND unitId = ? GROUP BY lessonID";

    //GET STUDENT's PEERS IN LESSON
    public static final String GET_DISTINCT_COLLID_FOR_LESSONID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(collectionId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND collectionType =?";


    public static final String GET_DISTINCT_USERS_FOR_COLLECTION_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(actorID) AS Users, collectionId FROM BaseReports "
            + "WHERE collectionId = ? AND collectionType =?";
    
    public static final String GET_USERS_LATEST_TIMESTAMP = 
    		"SELECT updateTimestamp, actorId from basereports "
    		+ " WHERE lessonId = ? AND collectionId = ? AND collectionType =? AND actorId = ? ORDER BY updateTimestamp DESC LIMIT 1";

  //*************************************************************************************************************************    
    //GET SESSION STATUS    
    /**public static final String GET_SESSION_STATUS =  "SELECT count(*) from BaseReports WHERE sessionID = ? "
    		+ " AND collectionID = ? AND EventType = ? AND EventName = ? "; **/
    
    public static final String GET_SESSION_STATUS =  "SELECT eventName, eventType, CreateTimeStamp from BaseReports WHERE sessionID = ? "
	+ " AND collectionID = ? AND EventName = ? ";
    
    //GET USER ALL SESSIONS FROM ASSESSMENT    
    public static final String GET_USER_SESSIONS_FOR_COLLID =  "SELECT DISTINCT(sessionID) from BaseReports WHERE collectionID = ? "
    		+ " AND collectiontype = ? AND actorID = ? ";
    
    
  //*************************************************************************************************************************
    // TEACHER REPORTS
    //String Constants and Queries for ALL STUDENT PERFORMANCE REPORTS IN COURSE

    public static final String SELECT_DISTINCT_UNITID_FOR_COURSEID_FILTERBY_COLLTYPE_FORALLUSERS =
            "SELECT DISTINCT(unitId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND collectionType =?";

    public static final String SELECT_STUDENT_COURSE_PERF_FOR_ASSESSMENT_FORALLUSERS =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(score) AS scoreInPercentage, SUM(collectionViews) AS attempts, "
            + "SUM(reaction) AS reaction, unitId FROM BaseReports "
            + "WHERE unitId = ANY(?::varchar[]) AND collectionType =? GROUP BY unitId";
    
    public static final String SELECT_STUDENT_COURSE_PERF_FOR_COLLECTION_FORALLUSERS =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(collectionViews) AS views, SUM(reaction) AS reaction, unitId FROM BaseReports "
            + "WHERE unitId = ANY(?::varchar[]) AND collectionType =? GROUP BY unitId";
    
    public static final String GET_COMPLETED_COLLID_COUNT_FOREACH_UNITID_FORALLUSERS = 
    		"SELECT COUNT(collectionId) as completedCount, unitId from basereports "
    		+ "WHERE classId = ? AND courseId = ? AND collectionType =? AND eventName = ? AND eventType = ? GROUP BY unitId";
    
    //*************************************************************************************************************************
    //String Constants and Queries for ALL STUDENT PERFORMANCE REPORTS IN UNIT
    
    public static final String SELECT_DISTINCT_LESSONID_FOR_UNITID_FITLERBY_COLLTYPE_FORALLUSERS =
            "SELECT DISTINCT(lessonId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND unitId = ? AND collectionType = ?";
    
    public static final String SELECT_DISTINCT_COLLID_FOR_LESSONID_FILTERBY_COLLTYPE_FORALLUSERS =
            "SELECT DISTINCT(collectionId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND collectionType =?";

    public static final String SELECT_STUDENT_EACH_UNIT_PERF_FOR_ASSESSMENT_FORALLUSERS =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(score) AS scoreInPercentage, SUM(reaction) AS reaction, lessonId FROM BaseReports "
            + "WHERE lessonId = ? GROUP BY lessonId";
    
    public static final String SELECT_STUDENT_EACH_UNIT_PERF_FOR_COLLECTION_FORALLUSERS =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(collectionViews) AS views, SUM(reaction) AS reaction, lessonId FROM BaseReports "
            + "WHERE lessonId = ? GROUP BY lessonId";
    
    public static final String GET_COMPLETED_COLLID_COUNT_FOREACH_LESSONID_FORALLUSERS = 
    		"SELECT COUNT(collectionId) as completedCount, lessonId from basereports "
    		+ "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND collectionType = ? "
    		+ "AND eventName = ? AND eventType = ? GROUP BY lessonId";
    
    public static final String SELECT_STUDENT_LESSON_PERF_FOR_ASSESSMENT_FORALLUSERS =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(score) AS scoreInPercentage, SUM(reaction) AS reaction, "
            + "SUM(collectionViews) AS attempts, collectionId FROM BaseReports "
            + "WHERE collectionId = ANY(?::varchar[]) GROUP BY collectionId";
    
    public static final String SELECT_STUDENT_LESSON_PERF_FOR_COLLECTION_FORALLUSERS =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(reaction) AS reaction, "
            + "SUM(collectionViews) AS views, collectionId FROM BaseReports "
            + "WHERE collectionId = ANY(?::varchar[]) GROUP BY collectionId";
    
    public static final String SELECT_CLASS_USER_BY_SESSION_ID = "SELECT classid,actorid FROM basereports WHERE sessionid = ? LIMIT 1";
    
    //*************************************************************************************************************************
    public static final String UUID_TYPE = "uuid";
   
}
