package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author Gooru
 *
 */
public class ResponseAttributeIdentifier {

  private static final Map<String, String> sessionTaxReportAggAttributes;
  private static final Map<String, String> sessionTaxReportQuestionAttributes;

  private static final Map<String, String> sessionAssessmentAttributes;
  private static final Map<String, String> sessionAssessmentQuestionAttributes;

  private static final Map<String, String> sessionCollectionAttributes;
  private static final Map<String, String> sessionCollectionResouceAttributes;
  
  private static final Map<String, String> coursePerformanceAttributes;
  
  private static final Map<String, String> unitPerformanceAttributes;
  
  private static final Map<String, String> lessonPerformanceAttributes;
  
  static {
    sessionAssessmentAttributes = new HashMap<>();
   // sessionAssessmentAttributes.put("score", "score");
    sessionAssessmentAttributes.put("collection_id", "gooruOId");
    sessionAssessmentAttributes.put("reaction", "reaction");
    sessionAssessmentAttributes.put("collectionTimeSpent", "timeSpent");
    sessionAssessmentAttributes.put("updated_at", "eventTime");
    sessionAssessmentAttributes.put("session_id", "sessionId");
    sessionAssessmentAttributes.put("collection_type", "resourceType");
    sessionAssessmentAttributes.put("collectionViews", "attempts");

    
  }
  static {
    sessionAssessmentQuestionAttributes = new HashMap<>();
    //sessionAssessmentQuestionAttributes.put("score", "score");
    sessionAssessmentQuestionAttributes.put("resource_id", "gooruOId");
    sessionAssessmentQuestionAttributes.put("reaction", "reaction");
    sessionAssessmentQuestionAttributes.put("resourceTimeSpent", "timeSpent");
    sessionAssessmentQuestionAttributes.put("updated_at", "eventTime");
    sessionAssessmentQuestionAttributes.put("session_id", "sessionId");
    sessionAssessmentQuestionAttributes.put("resource_type", "resourceType");
    sessionAssessmentQuestionAttributes.put("question_type", "questionType");
    sessionAssessmentQuestionAttributes.put("resourceViews", "views");

  }
  
  static {
    sessionCollectionAttributes = new HashMap<>();
    //sessionCollectionAttributes.put("score", "score");
    sessionCollectionAttributes.put("collection_id", "gooruOId");
    sessionCollectionAttributes.put("reaction", "reaction");
    sessionCollectionAttributes.put("collectionTimeSpent", "timeSpent");
   // sessionCollectionAttributes.put("updatetimestamp", "eventTime");
  //  sessionCollectionAttributes.put("sessionid", "sessionId");
   // sessionCollectionAttributes.put("collectiontype", "resourceType");
    sessionCollectionAttributes.put("collectionViews", "views");

    
  }
  static {
    sessionCollectionResouceAttributes = new HashMap<>();
    //sessionCollectionResouceAttributes.put("score", "score");
    sessionCollectionResouceAttributes.put("resource_id", "gooruOId");
    sessionCollectionResouceAttributes.put("reaction", "reaction");
    sessionCollectionResouceAttributes.put("resourceTimeSpent", "timeSpent");
    sessionCollectionResouceAttributes.put("created_at", "eventTime");
    //sessionCollectionResouceAttributes.put("sessionid", "sessionId");
    sessionCollectionResouceAttributes.put("resource_type", "resourceType");
    sessionCollectionResouceAttributes.put("question_type", "questionType");
    sessionCollectionResouceAttributes.put("resourceViews", "views");

  }
  
  static {
    sessionTaxReportAggAttributes = new HashMap<>();
    sessionTaxReportAggAttributes.put("display_code", "displayCode");
    sessionTaxReportAggAttributes.put("time_spent", "timespent");
    //sessionTaxReportAggAttributes.put("score", "score");
    sessionTaxReportAggAttributes.put("reaction", "reaction");

  }
  
  static {
    sessionTaxReportQuestionAttributes = new HashMap<>();
    sessionTaxReportQuestionAttributes.put("resource_id", "questionId");
    sessionTaxReportQuestionAttributes.put("time_spent", "timespent");
    sessionTaxReportQuestionAttributes.put("views", "attempts");
    //sessionTaxReportQuestionAttributes.put("score", "score");
    sessionTaxReportQuestionAttributes.put("question_type", "questionType");
    sessionTaxReportQuestionAttributes.put("resource_attempt_status","answerStatus");
    sessionTaxReportQuestionAttributes.put("reaction", "reaction");
  }

  static {
    lessonPerformanceAttributes = new HashMap<>();
    lessonPerformanceAttributes.put("timeSpent", "timeSpent");
   // lessonPerformanceAttributes.put("scoreInPercentage", "scoreInPercentage");
    lessonPerformanceAttributes.put("attempts", "attempts");
    lessonPerformanceAttributes.put("reaction", "reaction");
    lessonPerformanceAttributes.put("collectionId", "assessmentId");
    lessonPerformanceAttributes.put("attemptStatus", "attemptStatus");
  }
  
  static {
    coursePerformanceAttributes = new HashMap<>();
    coursePerformanceAttributes.put("timeSpent", "timeSpent");
    coursePerformanceAttributes.put("attempts", "attempts");
    coursePerformanceAttributes.put("reaction", "reaction");
    coursePerformanceAttributes.put("unit_id", "unitId");
    coursePerformanceAttributes.put("attemptStatus", "attemptStatus");
  }
  
  static {
    unitPerformanceAttributes = new HashMap<>();
    unitPerformanceAttributes.put("timeSpent", "timeSpent");
    unitPerformanceAttributes.put("attempts", "attempts");
    unitPerformanceAttributes.put("reaction", "reaction");
    unitPerformanceAttributes.put("lessonId", "lessonId");
    unitPerformanceAttributes.put("attemptStatus", "attemptStatus");
  }
  
  public static Map<String, String> getSessionTaxReportAggAttributesMap() {
    return sessionTaxReportAggAttributes;
  }
  public static Map<String, String> getSessionTaxReportQuestionAttributesMap() {
    return sessionTaxReportQuestionAttributes;
  }
  public static Map<String, String> getSessionAssessmentAttributesMap() {
    return sessionAssessmentAttributes;
  }
  public static Map<String, String> getSessionAssessmentQuestionAttributesMap() {
    return sessionAssessmentQuestionAttributes;
  }
  
  public static Map<String, String> getSessionCollectionAttributesMap() {
    return sessionCollectionAttributes;
  }
  public static Map<String, String> getSessionCollectionResourceAttributesMap() {
    return sessionCollectionResouceAttributes;
  }
  public static Map<String, String> getCoursePerformanceAttributesMap() {
    return coursePerformanceAttributes;
  }
  public static Map<String, String> getUnitPerformanceAttributesMap() {
    return unitPerformanceAttributes;
  }
  public static Map<String, String> getLessonPerformanceAttributesMap() {
    return lessonPerformanceAttributes;
  }
}
