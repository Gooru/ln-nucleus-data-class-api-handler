package org.gooru.nucleus.handlers.dataclass.api.processors.suggestions;

import java.util.List;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCollectionPerformance;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author renuka
 */
class SuggestionsPerformanceResponseBuilderImpl implements SuggestionsPerformanceResponseBuilder {

  private final List<AJEntityCollectionPerformance> items;
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SuggestionsPerformanceResponseBuilderImpl.class);

  SuggestionsPerformanceResponseBuilderImpl(List<AJEntityCollectionPerformance> items) {
    this.items = items;
  }

  @Override
  public JsonArray build() {
    JsonArray userUsageArray = new JsonArray();
    userUsageArray = buildActivityResponse(userUsageArray, items);
    return userUsageArray;
  }

  private JsonArray buildActivityResponse(JsonArray userUsageArray,
      List<AJEntityCollectionPerformance> items) {
    for (AJEntityCollectionPerformance item : items) {
      JsonObject activityKpi = new JsonObject();
      String activityType = item.getCollectionType();
      Boolean isSessionComplete = item.getStatus();

      activityKpi.put(AJEntityBaseReports.ATTR_COLLECTION_TYPE, activityType);
      String collId = item.getCollectionId();
      activityKpi.put(AJEntityBaseReports.ATTR_COLLECTION_ID, collId);
      Object collTitle = Base.firstCell(AJEntityContent.GET_TITLE, collId);
      activityKpi.put(JsonConstants.TITLE, (collTitle != null ? collTitle : "NA"));
      activityKpi.put(AJEntityBaseReports.ATTR_SESSION_ID, item.getSessionId());
      activityKpi.put(AJEntityBaseReports.ATTR_CONTENT_SOURCE, item.getContentSource());
      activityKpi.put(AJEntityBaseReports.ATTR_CLASS_ID, item.getClassId());
      activityKpi.put(AJEntityBaseReports.ATTR_COURSE_ID, item.getCourseId());
      activityKpi.put(AJEntityBaseReports.ATTR_UNIT_ID, item.getUnitId());
      activityKpi.put(AJEntityBaseReports.ATTR_LESSON_ID, item.getLessonId());
      activityKpi.put(AJEntityBaseReports.ATTR_PATH_ID, item.getPathId());
      activityKpi.put(AJEntityBaseReports.ATTR_PATH_TYPE, item.getPathType());
      activityKpi.put(AJEntityBaseReports.ATTR_LAST_ACCESSED, item.getUpdatedAt());
      activityKpi.put(AJEntityBaseReports.ATTR_TIME_SPENT, item.getTimespent());
      activityKpi.put(AJEntityBaseReports.ATTR_REACTION, item.getReaction());
      activityKpi.put(JsonConstants.STATUS,
          isSessionComplete ? JsonConstants.COMPLETE : JsonConstants.IN_PROGRESS);

      Double score = item.getScore();
      Double maxScore = item.getMaxScore();
      Integer attempts = item.getViews();
      if (activityType.equalsIgnoreCase(JsonConstants.COLLECTION)) {
        if (!AJEntityCollectionPerformance.isValidScoreForCollection(score, maxScore)) {
          score = null;
        }
        activityKpi.put(EventConstants.VIEWS, (attempts != null && attempts > 0) ? attempts : 1);
      } else {
        if (!isSessionComplete) {
          continue;
        }
        activityKpi.put(AJEntityBaseReports.ATTR_ATTEMPTS, attempts);
      }
      activityKpi.put(AJEntityBaseReports.ATTR_SCORE, score != null ? Math.round(score) : null);

      String gradingStatus = null;
      if (item.getBoolean(AJEntityBaseReports.IS_GRADED) != null) {
        gradingStatus = JsonConstants.COMPLETE;
        if (!item.getBoolean(AJEntityBaseReports.IS_GRADED)) {
          gradingStatus = JsonConstants.IN_PROGRESS;
        }
      }
      activityKpi.put(AJEntityBaseReports.ATTR_GRADE_STATUS, gradingStatus);
      userUsageArray.add(activityKpi);
    }
    return userUsageArray;
  }

}
