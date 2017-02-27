package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCourseCollectionCount;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.util.StringUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class StudentPerfInAllClasses implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(StudentPerfInAllClasses.class);

  private final ProcessorContext context;
  private static final String REQUEST_USERID = "userId";
  private String userId;
  private JsonArray classIds;

  StudentPerfInAllClasses(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    // No Sanity Check required since, no params are being passed in Request
    // Body
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    LOGGER.debug("validateRequest() OK");
    // FIXME :: Teacher validation to be added.
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {

    this.userId = this.context.request().getString(REQUEST_USERID);
    this.classIds = this.context.request().getJsonArray(EventConstants.CLASS_IDS);
    LOGGER.debug("userId : {} - classIds:{}", userId, this.classIds);

    if (classIds.isEmpty()) {
      LOGGER.warn("ClassIds are mandatory to fetch Student Performance in Classess");
      return new ExecutionResult<>(
              MessageResponseFactory.createInvalidRequestResponse("ClassIds is Missing. Cannot fetch Student Performance in Classes"),
              ExecutionStatus.FAILED);
    }

    JsonObject result = new JsonObject();
    JsonArray ClassKpiArray = new JsonArray();

    // Getting timespent and attempts.
    List<Map> classPerfData = null;
    if (!StringUtil.isNullOrEmpty(this.userId)) {
      classPerfData = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_ALL_CLASS_DATA, listToPostgresArrayString(this.classIds), this.userId);
    } else {
      classPerfData = Base.findAll(AJEntityBaseReports.SELECT_ALL_STUDENT_ALL_CLASS_DATA, listToPostgresArrayString(this.classIds));
    }
    if (!classPerfData.isEmpty()) {
      classPerfData.forEach(classData -> {
        JsonObject classKPI = new JsonObject();
        classKPI.put(AJEntityBaseReports.CLASS_GOORU_OID, classData.get(AJEntityBaseReports.CLASS_GOORU_OID).toString());
        classKPI.put(AJEntityBaseReports.ATTR_TIMESPENT, Integer.valueOf(classData.get(AJEntityBaseReports.ATTR_TIMESPENT).toString()));
        classKPI.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, 0);
        classKPI.put(AJEntityBaseReports.ATTR_SCORE, 0);
        Object classTotalCount = Base.firstCell(AJEntityCourseCollectionCount.GET_CLASS_ASSESSMENT_COUNT,
                classData.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
        classKPI.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, classTotalCount != null ? Integer.valueOf(classTotalCount.toString()) : 0);
        List<Map> classScoreCompletion = null;
        if (!StringUtil.isNullOrEmpty(this.userId)) {
          classScoreCompletion = Base.findAll(AJEntityBaseReports.SELECT_STUDENT_ALL_CLASS_COMPLETION_SCORE,
                  classData.get(AJEntityBaseReports.CLASS_GOORU_OID).toString(), this.userId);
        } else {
          classScoreCompletion = Base.findAll(AJEntityBaseReports.SELECT_ALL_STUDENT_ALL_CLASS_COMPLETION_SCORE,
                  classData.get(AJEntityBaseReports.CLASS_GOORU_OID).toString());
        }
        if (!classScoreCompletion.isEmpty()) {
          classScoreCompletion.forEach(scoreKPI -> {
            LOGGER.debug("completedCount : {} ", scoreKPI.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT));
            LOGGER.debug("score : {} ", scoreKPI.get(AJEntityBaseReports.ATTR_SCORE));
            classKPI.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT,
                    Integer.valueOf(scoreKPI.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));
            classKPI.put(AJEntityBaseReports.ATTR_SCORE, Integer.valueOf(scoreKPI.get(AJEntityBaseReports.ATTR_SCORE).toString()));
          });
        }
        ClassKpiArray.add(classKPI);
      });
    } else {
      LOGGER.info("Could not get Student Class Performance");
    }

    // Form the required Json pass it on
    result.put(JsonConstants.USAGE_DATA, ClassKpiArray).put(JsonConstants.USERID, this.userId);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);
  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }

  private String listToPostgresArrayString(JsonArray inputArrary) {
    List<String> input = new ArrayList<>();
    for (Object s : inputArrary) {
      input.add(s.toString());
    }
    int approxSize = ((input.size() + 1) * 36);
    Iterator<String> it = input.iterator();
    if (!it.hasNext()) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder(approxSize);
    sb.append('{');
    for (;;) {
      String s = it.next();
      sb.append('"').append(s).append('"');
      if (!it.hasNext()) {
        return sb.append('}').toString();
      }
      sb.append(',');
    }
  }

}
