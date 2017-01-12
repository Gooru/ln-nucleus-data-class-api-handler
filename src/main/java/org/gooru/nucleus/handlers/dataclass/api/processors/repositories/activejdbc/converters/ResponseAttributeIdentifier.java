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

  private static final List<Map<String, Object>> sample;

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

  static {
    sample = new ArrayList<>();
    Map<String, Object> sampleData = new HashMap<>();
    sampleData.put("standard_id", "standardsId");
    sampleData.put("learning_target_id", "learningTargetId");
    sampleData.put("display_code", "displayCode");
    sampleData.put("agg_time_spent", 345567);
    sampleData.put("agg_score", 70);
    
    sampleData.put("resource_id", "questionId");
    sampleData.put("time_spent", 2000);
    sampleData.put("views", 2);
    sampleData.put("score", 60);
    sampleData.put("question_type", "MC");
    sampleData.put("resource_attempt_status","completed");
    sampleData.put("reaction", 0);
    
    Map<String, Object> sampleData2 = new HashMap<>();
    sampleData2.put("standard_id", "standardsId2");
    sampleData2.put("learning_target_id", "learningTargetId2");
    sampleData2.put("display_code", "displayCode");
    sampleData2.put("agg_time_spent", 1234);
    sampleData2.put("agg_score", 80);
    
    sampleData2.put("resource_id", "questionId");
    sampleData2.put("time_spent", 55000);
    sampleData2.put("views", 5);
    sampleData2.put("score", 70);
    sampleData2.put("question_type", "MC");
    sampleData2.put("resource_attempt_status","completed");
    sampleData2.put("reaction", 5);
    sample.add(sampleData);
    sample.add(sampleData2);
  }
  public static Map<String, String> getSessionTaxReportAggAttributesMap() {
    return sessionTaxReportAggAttributes;
  }
  public static Map<String, String> getSessionTaxReportQuestionAttributesMap() {
    return sessionTaxReportQuestionAttributes;
  }
  
  /*public static void main(String a[]) {
    JsonArray finalData = new JsonArray();
    for (Map<String, Object> sampleData : sample) {
      Map<String, Object> nestedObj = new HashMap<>();
      JsonObject responseData = new JsonObject();
      for (Entry<String, String> entry : sessionTaxReportAggAttributes.entrySet()) {
        if (entry.getValue().contains("~")) { 
          String[] values = entry.getValue().split("~");
          Map<String, Object> nested = null;
          if (nestedObj.get(values[0]) != null) { 
            nested =  (Map<String, Object>) nestedObj.get(values[0]);
          } else { 
            nested = new HashMap<String, Object>(); 
          }
          nested.put(values[1], sampleData.get(entry.getKey()));
          nestedObj.put(values[0], nested);
          responseData.put(values[0], nested);
        } else {         
          responseData.put(entry.getValue(), sampleData.get(entry.getKey()));
        }
      }
      
      finalData.add(responseData);
    }
    System.out.println(finalData);
  }*/
}
