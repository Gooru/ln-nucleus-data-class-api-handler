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
    public static final String SELECT_DISTINCT_UNITID_FOR_COURSEID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(unitId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND collectionType =? AND actorId = ?";

    public static final String SELECT_DISTINCT_USERID_FOR_COURSEID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(actorId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND collectionType =?";
    
    public static final String SELECT_STUDENT_COURSE_PERF_FOR_ASSESSMENT =
              "SELECT SUM(agg.timeSpent) timeSpent, "
            + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.unitId, 'completed' AS attemptStatus "
            + "FROM (SELECT timeSpent AS timeSpent, "
            + "reaction AS reaction, views AS attempts, unitId FROM BaseReports "
            + "WHERE classid = ? AND courseid = ? AND collectionType =? AND actorId = ? AND unitId = ANY(?::varchar[]) AND "
            + "eventName = ? AND eventtype = 'stop') AS agg "
            + "GROUP BY agg.unitId";
    public static final String SELECT_STUDENT_COURSE_PERF_FOR_COLLECTION =
            "SELECT SUM(agg.timeSpent) timeSpent, "
          + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.unitId, 'completed' AS attemptStatus "
          + "FROM (SELECT timeSpent AS timeSpent, "
          + "reaction AS reaction, views AS attempts, unitId FROM BaseReports "
          + "WHERE classid = ? AND courseid = ? AND collectionType =? AND actorId = ? AND unitId = ANY(?::varchar[]) AND "
          + "eventName = ? ) AS agg "
          + "GROUP BY agg.unitId";
  
/*    public static final String SELECT_STUDENT_COURSE_PERF_FOR_COLLECTION =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(collectionViews) AS views, SUM(reaction) AS reaction, unitId FROM BaseReports "
            + "WHERE unitId = ANY(?::varchar[]) AND collectionType =? AND actorId = ? GROUP BY unitId";
    */
    public static final String GET_COMPLETED_COLLID_COUNT_FOREACH_UNITID = 
    		"SELECT SUM(unitData.completion) AS completedCount, ROUND(AVG(scoreInPercentage)) scoreInPercentage FROM "
    		+ "(SELECT DISTINCT ON (collectionid) CASE  WHEN (eventtype = 'stop') THEN 1 ELSE 0 END AS completion,"
    		+ "FIRST_VALUE(score) OVER (PARTITION BY collectionid ORDER BY updatetimestamp desc) AS scoreInPercentage,"
    		+ "unitid FROM basereports WHERE classid = ? AND courseid = ? AND unitid = ? AND "
    		+ "collectionType =? AND actorId = ? AND eventName = ? AND eventtype = 'stop' ORDER BY collectionid, updatetimestamp DESC) "
    		+ "AS unitData GROUP BY unitid;";
    
    public static final String GET_COMPLETED_COLL_COUNT_FOREACH_UNITID = 
            "SELECT SUM(unitData.completion) AS completedCount, ROUND(AVG(scoreInPercentage)) scoreInPercentage FROM "
            + "(SELECT DISTINCT ON (collectionid) CASE  WHEN (eventtype = 'stop') THEN 1 ELSE 0 END AS completion,"
            + "FIRST_VALUE(score) OVER (PARTITION BY collectionid ORDER BY updatetimestamp desc) AS scoreInPercentage,"
            + "unitid FROM basereports WHERE classid = ? AND courseid = ? AND unitid = ? AND "
            + "collectionType =? AND actorId = ? AND eventName = ?  ORDER BY collectionid, updatetimestamp DESC) "
            + "AS unitData GROUP BY unitid;";
        
    
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
            + "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND collectionType =? AND actorId = ?";

    public static final String SELECT_DISTINCT_USERID_FOR_LESSONID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(actorId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND collectionType =?";

    public static final String SELECT_DISTINCT_USERID_FOR_COLLECTIONID_FILTERBY_COLLTYPE =
            "SELECT DISTINCT(actorId) FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND collectionId = ? AND collectionType =?";

    public static final String SELECT_STUDENT_EACH_UNIT_PERF_FOR_ASSESSMENT =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(score) AS scoreInPercentage, SUM(reaction) AS reaction, lessonId FROM BaseReports "
            + "WHERE lessonId = ? AND actorId = ? GROUP BY lessonId";
    
    public static final String SELECT_STUDENT_EACH_UNIT_PERF_FOR_COLLECTION =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(collectionViews) AS views, SUM(reaction) AS reaction, lessonId FROM BaseReports "
            + "WHERE lessonId = ? AND actorId = ? GROUP BY lessonId";
    
/*    public static final String GET_COMPLETED_COLLID_COUNT_FOREACH_LESSONID = 
    		"SELECT COUNT(collectionId) as completedCount, lessonId from basereports "
    		+ "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND collectionType = ? "
    		+ "AND actorId = ? AND eventName = ? AND eventType = ? GROUP BY lessonId";
*/
    public static final String GET_COMPLETED_COLLID_COUNT_FOREACH_LESSONID = 
            "SELECT SUM(lessonData.completion) AS completedCount,ROUND(AVG(scoreInPercentage)) scoreInPercentage FROM "
            + "(SELECT DISTINCT ON (collectionid) CASE  WHEN (eventtype = 'stop') THEN 1 ELSE 0 END AS completion, "
            + "lessonId, FIRST_VALUE(score) OVER (PARTITION BY collectionid ORDER BY updatetimestamp desc) AS scoreInPercentage "
            + "FROM basereports WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? "
            + " AND collectionType =? AND actorId = ? AND eventName = ? AND eventtype = 'stop' ORDER BY collectionid, updatetimestamp DESC) "
            + "AS lessonData GROUP BY lessonId;";
    public static final String GET_COMPLETED_COLL_COUNT_FOREACH_LESSONID = 
            "SELECT SUM(lessonData.completion) AS completedCount,ROUND(AVG(scoreInPercentage)) scoreInPercentage FROM "
            + "(SELECT DISTINCT ON (collectionid) CASE  WHEN (eventtype = 'stop') THEN 1 ELSE 0 END AS completion, "
            + "lessonId, FIRST_VALUE(score) OVER (PARTITION BY collectionid ORDER BY updatetimestamp desc) AS scoreInPercentage "
            + "FROM basereports WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? "
            + " AND collectionType =? AND actorId = ? AND eventName = ? ORDER BY collectionid, updatetimestamp DESC) "
            + "AS lessonData GROUP BY lessonId;";  
    public static final String SELECT_STUDENT_UNIT_PERF_FOR_ASSESSMENT =
            "SELECT SUM(agg.timeSpent) timeSpent, "
          + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.lessonId, 'completed' AS attemptStatus "
          + "FROM (SELECT timeSpent AS timeSpent, "
          + "reaction AS reaction, views AS attempts, lessonId FROM BaseReports "
          + "WHERE classid = ? AND courseid = ? AND unitid = ? AND collectionType =? AND actorId = ? AND lessonId = ANY(?::varchar[]) AND "
          + "eventName = ? AND eventtype = 'stop') AS agg "
          + "GROUP BY agg.lessonId";
  
    public static final String SELECT_STUDENT_UNIT_PERF_FOR_COLLECTION =
            "SELECT SUM(agg.timeSpent) timeSpent, "
          + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.lessonId, 'completed' AS attemptStatus "
          + "FROM (SELECT timeSpent AS timeSpent, "
          + "reaction AS reaction, views AS attempts, lessonId FROM BaseReports "
          + "WHERE classid = ? AND courseid = ? AND unitid = ? AND collectionType =? AND actorId = ? AND lessonId = ANY(?::varchar[]) AND "
          + "eventName = ?) AS agg "
          + "GROUP BY agg.lessonId";
    
    //*************************************************************************************************************************
    //String Constants and Queries for STUDENT PERFORMANCE REPORTS IN LESSON
    public static final String SELECT_STUDENT_LESSON_PERF =
            "SELECT coalesce(SUM(collectionTimeSpent),0) AS timeSpent, SUM(score) AS scoreInPercentage, SUM(reaction) AS reaction, "
            + "coalesce(SUM(collectionViews),0) AS attempts, collectionId, case  when (eventtype = 'stop') then 'completed' else 'in-progress' end as attemptStatus FROM BaseReports "
            + "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND "
            + "collectiontype = ? AND actorId = ? GROUP BY collectionId,eventtype";
    
   /* public static final String SELECT_STUDENT_LESSON_PERF_FOR_ASSESSMENT =
                        "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(score) AS scoreInPercentage, SUM(reaction) AS reaction, "
                       + "SUM(collectionViews) AS attempts, collectionId FROM BaseReports "
                        + "WHERE collectionId = ANY(?::varchar[]) AND actorId = ? GROUP BY collectionId";*/
    
    public static final String SELECT_STUDENT_LESSON_PERF_FOR_ASSESSMENT =
            "SELECT SUM(agg.timeSpent) timeSpent, ROUND(AVG(agg.scoreInPercentage)) scoreInPercentage, "
          + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.collectionId, 'completed' AS attemptStatus "
          + "FROM (SELECT timeSpent AS timeSpent, FIRST_VALUE(score) OVER (PARTITION BY collectionid ORDER BY updatetimestamp desc) "
          + "AS scoreInPercentage, reaction AS reaction, views AS attempts, collectionId FROM BaseReports "
          + "WHERE classid = ? AND courseid = ? AND unitId = ? AND lessonId = ? AND collectionId = ANY(?::varchar[]) AND actorId = ? AND "
          + "eventName = ? AND eventtype = 'stop') AS agg "
          + "GROUP BY agg.collectionId";
    
    public static final String SELECT_STUDENT_LESSON_PERF_FOR_COLLECTION =
            "SELECT SUM(agg.timeSpent) timeSpent, ROUND(AVG(agg.scoreInPercentage)) scoreInPercentage, "
          + "SUM(agg.reaction) reaction, SUM(agg.attempts) attempts, agg.collectionId, 'completed' AS attemptStatus "
          + "FROM (SELECT timeSpent AS timeSpent, FIRST_VALUE(score) OVER (PARTITION BY collectionid ORDER BY updatetimestamp desc) "
          + "AS scoreInPercentage, reaction AS reaction, views AS attempts, collectionId FROM BaseReports "
          + "WHERE classid = ? AND courseid = ? AND unitId = ? AND lessonId = ? AND collectionId = ANY(?::varchar[]) AND actorId = ? AND "
          + "eventName = ? ) AS agg "
          + "GROUP BY agg.collectionId";
   /* public static final String SELECT_STUDENT_LESSON_PERF_FOR_COLLECTION =
            "SELECT SUM(collectionTimeSpent) AS timeSpent, SUM(reaction) AS reaction, "
            + "SUM(collectionViews) AS views, collectionId FROM BaseReports "
            + "WHERE collectionId = ANY(?::varchar[]) AND actorId = ? GROUP BY collectionId";*/
    
    public static final String GET_COMPLETED_COLLID_COUNT = 
    		"SELECT COUNT(collectionId) as completedCount, collectionId from basereports "
    		+ "WHERE classId = ? AND courseId = ? AND unitId = ? AND lessonId = ? AND "
    		+ "collectionType = ? AND actorId = ? AND eventName = ? AND eventType = ? GROUP BY collectionId";
    
    //*************************************************************************************************************************
    //STUDENT PERFORMANCE in Assessment
    public static final String GET_LATEST_COMPLETED_SESSION_ID = "SELECT sessionid AS sessionId FROM basereports WHERE"
            +" classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? AND collectionid = ? AND actorid = ? AND"
            +" eventname = 'collection.play' AND eventtype = 'stop'"
            +" ORDER BY createtimestamp ASC";
    
    //*************************************************************************************************************************
    //String Constants and Queries for STUDENT PERFORMANCE REPORTS IN ASSESSMENTS    
    public static final String SELECT_ASSESSMENT_FOREACH_COLLID_AND_SESSIONID =
            "select distinct on (collectionid) FIRST_VALUE(score) OVER (PARTITION BY collectionid ORDER BY updatetimestamp desc) AS score,"
            + "collectionid,FIRST_VALUE(reaction) OVER (PARTITION BY collectionid ORDER BY updatetimestamp desc) AS reaction,"
            + "FIRST_VALUE(timespent) OVER (PARTITION BY collectionid ORDER BY updatetimestamp desc) as collectiontimespent,"
            + "updatetimestamp,sessionid,collectiontype,FIRST_VALUE(views) OVER (PARTITION BY collectionid ORDER BY updatetimestamp asc) AS collectionviews "
            + "from basereports WHERE sessionId = ? AND eventName = ? ";
    
    
    public static final String SELECT_ASSESSMENT_QUESTION_FOREACH_COLLID_AND_SESSIONID =
            "select  distinct on (resourceid) FIRST_VALUE(score * 100) OVER (PARTITION BY resourceid ORDER BY updatetimestamp desc) AS score,"
            + "resourceid,FIRST_VALUE(reaction) OVER (PARTITION BY resourceid ORDER BY updatetimestamp desc) AS reaction,"
            + "FIRST_VALUE(timespent) OVER (PARTITION BY resourceid ORDER BY updatetimestamp desc) as resourcetimespent,"
            + "updatetimestamp,sessionid,collectiontype,"
            + "FIRST_VALUE(views) OVER (PARTITION BY resourceid ORDER BY updatetimestamp asc) AS resourceviews, "
            + "resourcetype,questiontype,"
            + "FIRST_VALUE(answerobject) OVER (PARTITION BY resourceid ORDER BY updatetimestamp desc) as answerobject "
            + "from basereports WHERE sessionId = ? AND eventname = ? ";
    
    public static final String SELECT_COLLECTION_FOREACH_COLLID_AND_SESSIONID =
            "SELECT distinct on (collectionid) score,collectionid,reaction,timespent AS collectiontimespent,createtimestamp,sessionid,collectiontype,views AS collectionviews from basereports"
            + " WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? AND collectionid = ? AND actorid = ? AND eventName = ?"
            ;
    
    public static final String SELECT_COLLECTION_RESOURCE_FOREACH_COLLID_AND_SESSIONID =
            "SELECT distinct on (resourceid) score,collectionid,reaction,timespent AS resourcetimespent,createtimestamp,sessionid,collectiontype,views AS resourceviews,resourcetype,questiontype,answerobject as answerobject from basereports"
            + " WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? AND collectionid = ? AND actorid = ? AND eventName = ?"
            ; 
  //*************************************************************************************************************************
    //Collection Summary report Queries
    //Getting collection question count
    public static final String SELECT_COLLECTION_QUESTION_COUNT = "SELECT question_count,updatetimestamp FROM BaseReports "
            + "WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? AND collectionid = ? AND actorid = ? AND eventname = 'collection.play'"
            + " ORDER BY updatetimestamp DESC LIMIT 1";
    //Getting COLLECTION DATA (views, timespent)
    public static final String SELECT_COLLECTION_AGG_DATA = "SELECT SUM(agg.timespent) AS collectiontimespent, SUM(agg.views) AS collectionviews,"
            + "agg.collectionId, agg.completionStatus, 0 AS score, 0 AS reaction FROM "
            + "(SELECT collectionid,timespent,sessionid,views,"
            + "CASE  WHEN (FIRST_VALUE(eventtype) OVER (PARTITION BY collectionid ORDER BY updatetimestamp desc) = 'stop') THEN 'completed' ELSE 'in-progress' END AS completionStatus "
            + "FROM BaseReports WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? AND collectionid = ? AND actorid = ? AND eventname = 'collection.play' ) AS agg "
            + "GROUP BY agg.collectionId,agg.completionStatus";
    //Getting COLLECTION DATA (score)
    public static final String SELECT_COLLECTION_AGG_SCORE = "SELECT SUM(agg.score) AS score FROM "
            + "(SELECT DISTINCT ON (resourceid) collectionid, "
            + "FIRST_VALUE(score) OVER (PARTITION BY resourceid ORDER BY updatetimestamp desc) AS score "
            + "FROM BaseReports WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? AND collectionid = ? AND actorid = ? AND "
            + "eventname = 'collection.resource.play' AND resourcetype = 'question' AND resourceattemptstatus <> 'skipped' ) AS agg "
            + "GROUP BY agg.collectionId";
    //Getting COLLECTION DATA (reaction)
    public static final String SELECT_COLLECTION_AGG_REACTION = "SELECT ROUND(AVG(agg.reaction)) AS reaction "
            + "FROM (SELECT DISTINCT ON (resourceid) collectionid,  "
            + "FIRST_VALUE(reaction) OVER (PARTITION BY resourceid ORDER BY updatetimestamp desc) "
            + "AS reaction FROM BaseReports "
            + "WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? AND collectionid = ? "
            + "AND actorid = ? AND eventname = 'collection.resource.play' AND reaction <> 0) AS agg GROUP BY agg.collectionId";
    //Getting RESOURCE DATA (views, timespent)
    public static final String SELECT_COLLECTION_RESOURCE_AGG_DATA = "SELECT collectionid, resourceid ,resourcetype,questiontype, SUM(views) AS resourceviews, "
            + "SUM(timespent) AS resourcetimespent, 0 as reaction, 0 as score, '[]' AS answerobject "
            + "FROM BaseReports WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? AND collectionid = ? "
            + "AND actorid = ? AND eventname = 'collection.resource.play' GROUP BY collectionid, resourceid,resourcetype,questiontype";
  //Getting RESOURCE DATA (score)
    public static final String SELECT_COLLECTION_QUESTION_AGG_SCORE = "SELECT DISTINCT ON (resourceid) "
            + "FIRST_VALUE(score) OVER (PARTITION BY resourceid "
            + "ORDER BY updatetimestamp desc) AS score,FIRST_VALUE(resourceattemptstatus) OVER (PARTITION BY resourceid ORDER BY updatetimestamp desc) AS attemptStatus, FIRST_VALUE(answerobject) OVER (PARTITION BY resourceid ORDER BY updatetimestamp desc) AS answerobject "
            + "FROM BaseReports WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? AND collectionid = ? AND resourceid = ?"
            + "AND actorid = ? AND eventname = 'collection.resource.play' AND resourcetype = 'question' AND resourceattemptstatus <> 'skipped'";
  //Getting RESOURCE DATA (reaction)
    public static final String SELECT_COLLECTION_RESOURCE_AGG_REACTION = "SELECT DISTINCT ON (resourceid) "
            + "FIRST_VALUE(reaction) OVER (PARTITION BY resourceid "
            + "ORDER BY updatetimestamp desc) AS reaction "
            + "FROM BaseReports WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? AND collectionid = ? AND resourceid = ? "
            + "AND actorid = ? AND eventname = 'collection.resource.play' AND reaction <> 0";
  
  //*************************************************************************************************************************
    //Collection Summary report Queries for context of outside class
    //Getting collection question count
  //Getting collection question count in the context of outside class
    public static final String SELECT_COLLECTION_QUESTION_COUNT_ = "SELECT question_count,updatetimestamp FROM BaseReports "
            + "WHERE classid IS NULL AND courseid IS NULL AND unitid IS NULL AND lessonid IS NULL AND collectionid = ? AND actorid = ? AND eventname = 'collection.play'"
            + " ORDER BY updatetimestamp DESC LIMIT 1";
    
    //Getting COLLECTION DATA (views, timespent)
    public static final String SELECT_COLLECTION_AGG_DATA_ = "SELECT SUM(agg.timespent) AS collectiontimespent, SUM(agg.views) AS collectionviews,"
            + "agg.collectionId, agg.completionStatus, 0 AS score, 0 AS reaction FROM "
            + "(SELECT collectionid,timespent,sessionid,views,"
            + "CASE  WHEN (FIRST_VALUE(eventtype) OVER (PARTITION BY collectionid ORDER BY updatetimestamp desc) = 'stop') THEN 'completed' ELSE 'in-progress' END AS completionStatus "
            + "FROM BaseReports WHERE classid IS NULL AND courseid IS NULL AND unitid IS NULL AND lessonid IS NULL AND collectionid = ? AND actorid = ? AND eventname = 'collection.play' ) AS agg "
            + "GROUP BY agg.collectionId,agg.completionStatus";
    //Getting COLLECTION DATA (score)
    public static final String SELECT_COLLECTION_AGG_SCORE_ = "SELECT SUM(agg.score) AS score FROM "
            + "(SELECT DISTINCT ON (resourceid) collectionid, "
            + "FIRST_VALUE(score) OVER (PARTITION BY resourceid ORDER BY updatetimestamp desc) AS score "
            + "FROM BaseReports WHERE classid IS NULL AND courseid IS NULL AND unitid IS NULL AND lessonid IS NULL AND collectionid = ? AND actorid = ? AND "
            + "eventname = 'collection.resource.play' AND resourcetype = 'question' AND resourceattemptstatus <> 'skipped' ) AS agg "
            + "GROUP BY agg.collectionId";
    //Getting COLLECTION DATA (reaction)
    public static final String SELECT_COLLECTION_AGG_REACTION_ = "SELECT ROUND(AVG(agg.reaction)) AS reaction "
            + "FROM (SELECT DISTINCT ON (resourceid) collectionid,  "
            + "FIRST_VALUE(reaction) OVER (PARTITION BY resourceid ORDER BY updatetimestamp desc) "
            + "AS reaction FROM BaseReports "
            + "WHERE classid IS NULL AND courseid IS NULL AND unitid IS NULL AND lessonid IS NULL AND collectionid = ? "
            + "AND actorid = ? AND eventname = 'collection.resource.play' AND reaction <> 0) AS agg GROUP BY agg.collectionId";
    //Getting RESOURCE DATA (views, timespent)
    public static final String SELECT_COLLECTION_RESOURCE_AGG_DATA_ = "SELECT collectionid, resourceid ,resourcetype,questiontype, SUM(views) AS resourceviews, "
            + "SUM(timespent) AS resourcetimespent, 0 as reaction, 0 as score, '[]' AS answerobject "
            + "FROM BaseReports WHERE classid IS NULL AND courseid IS NULL AND unitid IS NULL AND lessonid IS NULL AND collectionid = ? "
            + "AND actorid = ? AND eventname = 'collection.resource.play' GROUP BY collectionid, resourceid,resourcetype,questiontype";
  //Getting RESOURCE DATA (score)
    public static final String SELECT_COLLECTION_QUESTION_AGG_SCORE_ = "SELECT DISTINCT ON (resourceid) "
            + "FIRST_VALUE(score) OVER (PARTITION BY resourceid "
            + "ORDER BY updatetimestamp desc) AS score,FIRST_VALUE(resourceattemptstatus) OVER (PARTITION BY resourceid ORDER BY updatetimestamp desc) AS attemptStatus, FIRST_VALUE(answerobject) OVER (PARTITION BY resourceid ORDER BY updatetimestamp desc) AS answerobject "
            + "FROM BaseReports WHERE classid IS NULL AND courseid IS NULL AND unitid IS NULL AND lessonid IS NULL AND collectionid = ? AND resourceid = ?"
            + "AND actorid = ? AND eventname = 'collection.resource.play' AND resourcetype = 'question' AND resourceattemptstatus <> 'skipped'";
  //Getting RESOURCE DATA (reaction)
    public static final String SELECT_COLLECTION_RESOURCE_AGG_REACTION_ = "SELECT DISTINCT ON (resourceid) "
            + "FIRST_VALUE(reaction) OVER (PARTITION BY resourceid "
            + "ORDER BY updatetimestamp desc) AS reaction "
            + "FROM BaseReports WHERE classid IS NULL AND courseid IS NULL AND unitid IS NULL AND lessonid IS NULL AND collectionid = ? AND resourceid = ? "
            + "AND actorid = ? AND eventname = 'collection.resource.play' AND reaction <> 0";
      
  //*************************************************************************************************************************
      // GET CURRENT STUDENT LOCATITON
    public static final String GET_STUDENT_LOCATION = 
    		"select classid, courseid, unitid, lessonId, collectionId,collectiontype, createTimestamp, updateTimestamp from basereports "
    		+ " WHERE classId = ? AND actorId = ? ORDER BY updateTimestamp DESC LIMIT 1";
    
 // GET STUDENT's PEERS IN COURSE
    public static final String GET_PEERS_COUNT_IN_COURSE = 
        "SELECT count(aId) AS peerCount, unitId FROM "
        + "(SELECT DISTINCT ON (actorID) collectionId, courseId, lessonId, unitId as unitId, actorId as aId, "
        + "updateTimeStamp FROM basereports where classid = ? AND actorID <> ? "
        + "ORDER BY actorId, updatetimestamp DESC) AS DS "
        + "WHERE DS.courseId = ?  GROUP BY unitId;";

    // GET STUDENT's PEERS IN UNIT
    public static final String GET_PEERS_COUNT_IN_UNIT= 
        "SELECT count(aId) AS peerCount, lessonId FROM "
        + "(SELECT DISTINCT ON (actorID) collectionId, courseId, lessonId, unitId as unitId, actorId as aId, "
        + "updateTimeStamp FROM basereports where classid = ? AND actorID <> ? "
        + "ORDER BY actorId, updatetimestamp DESC) AS DS "
        + "WHERE  DS.courseId = ? AND DS.unitId = ?  GROUP BY lessonId";

  //GET STUDENT's PEERS IN LESSON
    public static final String GET_PEERS_IN_LESSON = "SELECT collectionId, aId as actorId, collectionType, updatetimestamp "
            + "FROM (SELECT DISTINCT ON (actorID) collectionId, courseId, lessonId, unitId as unitId, actorId as aId, collectionType, updateTimeStamp "
            + "FROM basereports where classid = ? AND actorID <> ? ORDER BY actorId, updatetimestamp DESC) AS DS "
            + "WHERE DS.courseId = ? AND DS.unitId = ?  AND lessonId = ?";

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
    public static final String GET_USER_SESSIONS_FOR_COLLID =  "SELECT DISTINCT(sessionID) from BaseReports WHERE classid = ? AND courseid = ? AND unitid = ? AND lessonid = ? AND collectionID = ? "
    		+ " AND collectiontype = ? AND actorID = ? ";
    
    //GET USER ALL SESSIONS FROM ASSESSMENT  OUT OF CLASS  
    public static final String GET_USER_SESSIONS_FOR_COLLID_ =  "SELECT DISTINCT(sessionID) from BaseReports WHERE classid IS NULL AND courseid IS NULL AND unitid IS NULL AND lessonid IS NULL AND collectionID = ? "
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
    //Student all classes performance
    
    //CLASS DATA FOR A USER (attempts, timespent)
    public static final String SELECT_STUDENT_ALL_CLASS_DATA = "SELECT SUM(timeSpent) AS timeSpent,  SUM(views) AS attempts, classId, courseId "
            + "FROM BaseReports WHERE classid = ANY(?::varchar[]) AND actorId = ? "
            + "AND eventName = 'collection.play' GROUP BY classid,courseId";

    //CLASS DATA FOR ALL USER(attempts, timespent)
    public static final String SELECT_ALL_STUDENT_ALL_CLASS_DATA = "SELECT SUM(timeSpent) AS timeSpent,  SUM(views) AS attempts, classId, courseId "
            + "FROM BaseReports WHERE classid = ANY(?::varchar[]) "
            + "AND eventName = 'collection.play' GROUP BY classid,courseId";
    
    //CLASS DATA FOR A USER(score, completion)
    public static final String SELECT_STUDENT_ALL_CLASS_COMPLETION_SCORE = "SELECT classId, SUM(classData.completion) AS completedCount, ROUND(AVG(scoreInPercentage)) AS scoreInPercentage "
            + "FROM (SELECT DISTINCT ON (collectionid) CASE  WHEN (eventtype = 'stop') THEN 1 ELSE 0 END AS completion, "
            + "FIRST_VALUE(score) OVER (PARTITION BY collectionid ORDER BY updatetimestamp desc) AS scoreInPercentage, classId "
            + "FROM basereports WHERE classId = ?  AND actorId = ? "
            + "AND eventName = 'collection.play' AND eventtype = 'stop' AND collectionType = 'assessment' "
            + "ORDER BY collectionid, updatetimestamp DESC) AS classData GROUP BY classId";

    //CLASS DATA FOR ALL USER(score, completion)
    public static final String SELECT_ALL_STUDENT_ALL_CLASS_COMPLETION_SCORE = "SELECT classId, SUM(classData.completion) AS completedCount, ROUND(AVG(scoreInPercentage)) AS scoreInPercentage "
            + "FROM (SELECT DISTINCT ON (collectionid) CASE  WHEN (eventtype = 'stop') THEN 1 ELSE 0 END AS completion, "
            + "FIRST_VALUE(score) OVER (PARTITION BY collectionid,actorId ORDER BY updatetimestamp desc) AS scoreInPercentage, classId "
            + "FROM basereports WHERE classId = ? "
            + "AND eventName = 'collection.play' AND eventtype = 'stop' AND collectionType = 'assessment' "
            + "ORDER BY collectionid, updatetimestamp DESC) AS classData GROUP BY classId";

    //*************************************************************************************************************************    
    //Student Location in All Classes    
    public static final String GET_STUDENT_LOCATION_ALL_CLASSES = "select DISTINCT ON (classID) classId, courseId, unitId, "
    		+ "lessonId, collectionId, collectionType, sessionId, updateTimeStamp FROM basereports WHERE actorid = ? AND classid = ANY(?::varchar[]) "
    		+ "ORDER BY classId, updatetimestamp DESC";
    
    public static final String GET_COLLECTION_STATUS =  "SELECT eventName, eventType from BaseReports WHERE sessionID = ? "
    		+ " AND collectionID = ? AND EventName = ? AND EventType = ?";

    //*************************************************************************************************************************
    public static final String UUID_TYPE = "uuid";
   
}
