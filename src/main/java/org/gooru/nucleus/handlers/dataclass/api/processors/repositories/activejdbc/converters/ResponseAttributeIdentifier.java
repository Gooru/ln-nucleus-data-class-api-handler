package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Gooru
 */
public final class ResponseAttributeIdentifier {

  private static final Map<String, String> sessionTaxReportAggAttributes;
  private static final Map<String, String> sessionTaxReportQuestionAttributes;
  private static final Map<String, String> sessionAssessmentAttributes;
  private static final Map<String, String> sessionDCAAssessmentAttributes;
  private static final Map<String, String> sessionAssessmentQuestionAttributes;
  private static final Map<String, String> sessionDCAAssessmentQuestionAttributes;
  private static final Map<String, String> sessionCollectionAttributes;
  private static final Map<String, String> sessionDCACollectionAttributes;
  private static final Map<String, String> sessionCollectionResouceAttributes;
  private static final Map<String, String> sessionDCACollectionResouceAttributes;
  private static final Map<String, String> coursePerformanceAttributes;
  private static final Map<String, String> unitPerformanceAttributes;
  private static final Map<String, String> lessonPerformanceAttributes;
  private static final Map<String, String> milestonePerformanceAttributes;
  private static final Map<String, String> milestoneLessonPerformanceAttributes;

  private static final Map<String, String> sessionTaxReportAggAttributesValue;
  private static final Map<String, String> sessionTaxReportQuestionAttributesValue;
  private static final Map<String, String> sessionAssessmentAttributesValue;
  private static final Map<String, String> sessionDCAAssessmentAttributesValue;
  private static final Map<String, String> sessionAssessmentQuestionAttributesValue;
  private static final Map<String, String> sessionDCAAssessmentQuestionAttributesValue;
  private static final Map<String, String> sessionCollectionAttributesValue;
  private static final Map<String, String> sessionDCACollectionAttributesValue;
  private static final Map<String, String> sessionCollectionResouceAttributesValue;
  private static final Map<String, String> sessionDCACollectionResouceAttributesValue;
  private static final Map<String, String> coursePerformanceAttributesValue;
  private static final Map<String, String> unitPerformanceAttributesValue;
  private static final Map<String, String> lessonPerformanceAttributesValue;
  private static final Map<String, String> milestonePerformanceAttributesValue;
  private static final Map<String, String> milestoneLessonPerformanceAttributesValue;

  static {
    sessionAssessmentAttributes = new HashMap<>();
    // sessionAssessmentAttributes.put("score", "score");
    sessionAssessmentAttributes.put("collection_id", "gooruOId");
    // sessionAssessmentAttributes.put("reaction", "reaction");
    sessionAssessmentAttributes.put("collectionTimeSpent", "timeSpent");
    sessionAssessmentAttributes.put("updated_at", "eventTime");
    sessionAssessmentAttributes.put("session_id", "sessionId");
    sessionAssessmentAttributes.put("collection_type", "resourceType");
    sessionAssessmentAttributes.put("collectionViews", "attempts");
    sessionAssessmentAttributesValue = Collections.unmodifiableMap(sessionAssessmentAttributes);
  }

  // DCA
  static {
    sessionDCAAssessmentAttributes = new HashMap<>();
    // sessionAssessmentAttributes.put("score", "score");
    sessionDCAAssessmentAttributes.put("collection_id", "assessmentId");
    // sessionDCAAssessmentAttributes.put("reaction", "reaction");
    sessionDCAAssessmentAttributes.put("collectionTimeSpent", "timeSpent");
    sessionDCAAssessmentAttributes.put("updated_at", "eventTime");
    sessionDCAAssessmentAttributes.put("session_id", "sessionId");
    sessionDCAAssessmentAttributes.put("collection_type", "collectionType");
    sessionDCAAssessmentAttributes.put("collectionViews", "attempts");
    sessionDCAAssessmentAttributesValue =
        Collections.unmodifiableMap(sessionDCAAssessmentAttributes);
  }

  static {
    sessionAssessmentQuestionAttributes = new HashMap<>();
    // sessionAssessmentQuestionAttributes.put("score", "score");
    sessionAssessmentQuestionAttributes.put("resource_id", "gooruOId");
    // sessionAssessmentQuestionAttributes.put("reaction", "reaction");
    sessionAssessmentQuestionAttributes.put("resourceTimeSpent", "timeSpent");
    sessionAssessmentQuestionAttributes.put("updated_at", "eventTime");
    sessionAssessmentQuestionAttributes.put("session_id", "sessionId");
    sessionAssessmentQuestionAttributes.put("resource_type", "resourceType");
    sessionAssessmentQuestionAttributes.put("question_type", "questionType");
    sessionAssessmentQuestionAttributes.put("resourceViews", "views");
    sessionAssessmentQuestionAttributesValue =
        Collections.unmodifiableMap(sessionAssessmentQuestionAttributes);
  }

  // DCA
  static {
    sessionDCAAssessmentQuestionAttributes = new HashMap<>();
    sessionDCAAssessmentQuestionAttributes.put("resource_id", "questionId");
    // sessionDCAAssessmentQuestionAttributes.put("reaction", "reaction");
    sessionDCAAssessmentQuestionAttributes.put("resourceTimeSpent", "timeSpent");
    sessionDCAAssessmentQuestionAttributes.put("updated_at", "eventTime");
    // sessionDCAAssessmentQuestionAttributes.put("session_id", "sessionId");
    sessionDCAAssessmentQuestionAttributes.put("resource_type", "resourceType");
    sessionDCAAssessmentQuestionAttributes.put("question_type", "questionType");
    sessionDCAAssessmentQuestionAttributes.put("resourceViews", "views");
    sessionDCAAssessmentQuestionAttributesValue =
        Collections.unmodifiableMap(sessionDCAAssessmentQuestionAttributes);
  }

  static {
    sessionCollectionAttributes = new HashMap<>();
    // sessionCollectionAttributes.put("score", "score");
    sessionCollectionAttributes.put("collection_id", "gooruOId");
    sessionCollectionAttributes.put("reaction", "reaction");
    sessionCollectionAttributes.put("collectionTimeSpent", "timeSpent");
    // sessionCollectionAttributes.put("updatetimestamp", "eventTime");
    // sessionCollectionAttributes.put("sessionid", "sessionId");
    // sessionCollectionAttributes.put("collectiontype", "resourceType");
    sessionCollectionAttributes.put("collectionViews", "views");
    sessionCollectionAttributesValue = Collections.unmodifiableMap(sessionCollectionAttributes);
  }

  // DCA
  static {
    sessionDCACollectionAttributes = new HashMap<>();
    sessionDCACollectionAttributes.put("collection_id", "collectionId");
    sessionDCACollectionAttributes.put("reaction", "reaction");
    sessionDCACollectionAttributes.put("collectionTimeSpent", "timeSpent");
    sessionDCACollectionAttributes.put("collectionViews", "views");
    sessionDCACollectionAttributesValue =
        Collections.unmodifiableMap(sessionDCACollectionAttributes);
  }

  static {
    sessionCollectionResouceAttributes = new HashMap<>();
    // sessionCollectionResouceAttributes.put("score", "score");
    sessionCollectionResouceAttributes.put("resource_id", "gooruOId");
    sessionCollectionResouceAttributes.put("reaction", "reaction");
    sessionCollectionResouceAttributes.put("resourceTimeSpent", "timeSpent");
    // sessionCollectionResouceAttributes.put("created_at", "eventTime");
    // sessionCollectionResouceAttributes.put("sessionid", "sessionId");
    sessionCollectionResouceAttributes.put("resource_type", "resourceType");
    sessionCollectionResouceAttributes.put("question_type", "questionType");
    sessionCollectionResouceAttributes.put("resourceViews", "views");
    sessionCollectionResouceAttributesValue =
        Collections.unmodifiableMap(sessionCollectionResouceAttributes);
  }

  // DCA
  static {
    sessionDCACollectionResouceAttributes = new HashMap<>();
    sessionDCACollectionResouceAttributes.put("resource_id", "resourceId");
    sessionDCACollectionResouceAttributes.put("reaction", "reaction");
    sessionDCACollectionResouceAttributes.put("resourceTimeSpent", "timeSpent");
    sessionDCACollectionResouceAttributes.put("created_at", "eventTime");
    sessionDCACollectionResouceAttributes.put("resource_type", "resourceType");
    sessionDCACollectionResouceAttributes.put("question_type", "questionType");
    sessionDCACollectionResouceAttributes.put("resourceViews", "views");
    sessionDCACollectionResouceAttributesValue =
        Collections.unmodifiableMap(sessionDCACollectionResouceAttributes);

  }

  static {
    sessionTaxReportAggAttributes = new HashMap<>();
    sessionTaxReportAggAttributes.put("display_code", "displayCode");
    sessionTaxReportAggAttributes.put("time_spent", "timespent");
    // sessionTaxReportAggAttributes.put("score", "score");
    sessionTaxReportAggAttributes.put("reaction", "reaction");
    sessionTaxReportAggAttributesValue = Collections.unmodifiableMap(sessionTaxReportAggAttributes);

  }

  static {
    sessionTaxReportQuestionAttributes = new HashMap<>();
    sessionTaxReportQuestionAttributes.put("resource_id", "questionId");
    sessionTaxReportQuestionAttributes.put("time_spent", "timespent");
    sessionTaxReportQuestionAttributes.put("views", "attempts");
    // sessionTaxReportQuestionAttributes.put("score", "score");
    sessionTaxReportQuestionAttributes.put("question_type", "questionType");
    sessionTaxReportQuestionAttributes.put("resource_attempt_status", "answerStatus");
    sessionTaxReportQuestionAttributes.put("reaction", "reaction");
    sessionTaxReportQuestionAttributesValue =
        Collections.unmodifiableMap(sessionTaxReportQuestionAttributes);
  }

  static {
    lessonPerformanceAttributes = new HashMap<>();
    lessonPerformanceAttributes.put("timeSpent", "timeSpent");
    // lessonPerformanceAttributes.put("scoreInPercentage", "scoreInPercentage");
    lessonPerformanceAttributes.put("attempts", "attempts");
    lessonPerformanceAttributes.put("reaction", "reaction");
    lessonPerformanceAttributes.put("collectionId", "assessmentId");
    lessonPerformanceAttributes.put("attemptStatus", "attemptStatus");
    lessonPerformanceAttributesValue = Collections.unmodifiableMap(lessonPerformanceAttributes);
  }

  static {
    coursePerformanceAttributes = new HashMap<>();
    coursePerformanceAttributes.put("timeSpent", "timeSpent");
    coursePerformanceAttributes.put("attempts", "attempts");
    coursePerformanceAttributes.put("reaction", "reaction");
    coursePerformanceAttributes.put("unit_id", "unitId");
    coursePerformanceAttributes.put("attemptStatus", "attemptStatus");
    coursePerformanceAttributesValue = Collections.unmodifiableMap(coursePerformanceAttributes);
  }

  static {
    unitPerformanceAttributes = new HashMap<>();
    unitPerformanceAttributes.put("timeSpent", "timeSpent");
    unitPerformanceAttributes.put("attempts", "attempts");
    unitPerformanceAttributes.put("reaction", "reaction");
    unitPerformanceAttributes.put("lessonId", "lessonId");
    unitPerformanceAttributes.put("attemptStatus", "attemptStatus");
    unitPerformanceAttributesValue = Collections.unmodifiableMap(unitPerformanceAttributes);
  }

  // MILESTONE
  static {
    milestoneLessonPerformanceAttributes = new HashMap<>();
    milestoneLessonPerformanceAttributes.put("timeSpent", "timeSpent");
    // lessonPerformanceAttributes.put("scoreInPercentage", "scoreInPercentage");
    milestoneLessonPerformanceAttributes.put("attempts", "attempts");
    milestoneLessonPerformanceAttributes.put("reaction", "reaction");
    milestoneLessonPerformanceAttributes.put("unit_id", "unitId");
    milestoneLessonPerformanceAttributes.put("lessonId", "lessonId");
    milestoneLessonPerformanceAttributesValue =
        Collections.unmodifiableMap(milestoneLessonPerformanceAttributes);
  }

  static {
    milestonePerformanceAttributes = new HashMap<>();
    milestonePerformanceAttributes.put("timeSpent", "timeSpent");
    milestonePerformanceAttributes.put("attempts", "attempts");
    milestonePerformanceAttributes.put("reaction", "reaction");
    milestonePerformanceAttributesValue =
        Collections.unmodifiableMap(milestonePerformanceAttributes);
  }


  private ResponseAttributeIdentifier() {
    throw new AssertionError();
  }

  public static Map<String, String> getSessionTaxReportAggAttributesMap() {
    return sessionTaxReportAggAttributesValue;
  }

  public static Map<String, String> getSessionTaxReportQuestionAttributesMap() {
    return sessionTaxReportQuestionAttributesValue;
  }

  public static Map<String, String> getSessionAssessmentAttributesMap() {
    return sessionAssessmentAttributesValue;
  }

  public static Map<String, String> getSessionAssessmentQuestionAttributesMap() {
    return sessionAssessmentQuestionAttributesValue;
  }

  public static Map<String, String> getSessionCollectionAttributesMap() {
    return sessionCollectionAttributesValue;
  }

  public static Map<String, String> getSessionCollectionResourceAttributesMap() {
    return sessionCollectionResouceAttributesValue;
  }

  // DCA
  public static Map<String, String> getSessionDCAAssessmentAttributesMap() {
    return sessionDCAAssessmentAttributesValue;
  }

  // DCA
  public static Map<String, String> getSessionDCAAssessmentQuestionAttributesMap() {
    return sessionDCAAssessmentQuestionAttributesValue;
  }

  // DCA
  public static Map<String, String> getSessionDCACollectionAttributesMap() {
    return sessionDCACollectionAttributesValue;
  }

  // DCA
  public static Map<String, String> getSessionDCACollectionResourceAttributesMap() {
    return sessionDCACollectionResouceAttributesValue;
  }

  public static Map<String, String> getCoursePerformanceAttributesMap() {
    return coursePerformanceAttributesValue;
  }

  public static Map<String, String> getUnitPerformanceAttributesMap() {
    return unitPerformanceAttributesValue;
  }

  public static Map<String, String> getLessonPerformanceAttributesMap() {
    return lessonPerformanceAttributesValue;
  }

  // MILESTONE
  public static Map<String, String> getMilestonePerformanceAttributesMap() {
    return milestonePerformanceAttributesValue;
  }

  public static Map<String, String> getMilestoneLessonPerformanceAttributesMap() {
    return milestoneLessonPerformanceAttributesValue;
  }

}
