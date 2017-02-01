package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.converters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

  static {
    sessionAssessmentAttributes = new HashMap<>();
    sessionAssessmentAttributes.put("score", "score");
    sessionAssessmentAttributes.put("collectionid", "gooruOId");
    sessionAssessmentAttributes.put("reaction", "reaction");
    sessionAssessmentAttributes.put("collectiontimespent", "timeSpent");
    sessionAssessmentAttributes.put("createtimestamp", "eventTime");
    sessionAssessmentAttributes.put("sessionid", "sessionId");
    sessionAssessmentAttributes.put("collectiontype", "resourceType");
    sessionAssessmentAttributes.put("collectionviews", "attempts");

    
  }
  static {
    sessionAssessmentQuestionAttributes = new HashMap<>();
    sessionAssessmentQuestionAttributes.put("score", "score");
    sessionAssessmentQuestionAttributes.put("collectionid", "gooruOId");
    sessionAssessmentQuestionAttributes.put("reaction", "reaction");
    sessionAssessmentQuestionAttributes.put("resourcetimespent", "timeSpent");
    sessionAssessmentQuestionAttributes.put("createtimestamp", "eventTime");
    sessionAssessmentQuestionAttributes.put("sessionid", "sessionId");
    sessionAssessmentQuestionAttributes.put("resourcetype", "resourceType");
    sessionAssessmentQuestionAttributes.put("questiontype", "questionType");
    sessionAssessmentQuestionAttributes.put("resourceviews", "views");
    //sessionAssessmentQuestionAttributes.put("answerobject", "answerObject");

  }
  static {
    sessionTaxReportAggAttributes = new HashMap<>();
    sessionTaxReportAggAttributes.put("display_code", "displayCode");
    sessionTaxReportAggAttributes.put("time_spent", "timespent");
    sessionTaxReportAggAttributes.put("score", "score");
    sessionTaxReportAggAttributes.put("reaction", "reaction");

  }
  
  static {
    sessionTaxReportQuestionAttributes = new HashMap<>();
    sessionTaxReportQuestionAttributes.put("resource_id", "questionId");
    sessionTaxReportQuestionAttributes.put("time_spent", "timespent");
    sessionTaxReportQuestionAttributes.put("views", "attempts");
    sessionTaxReportQuestionAttributes.put("score", "score");
    sessionTaxReportQuestionAttributes.put("question_type", "questionType");
    sessionTaxReportQuestionAttributes.put("resource_attempt_status","answerStatus");
    sessionTaxReportQuestionAttributes.put("reaction", "reaction");
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
}
