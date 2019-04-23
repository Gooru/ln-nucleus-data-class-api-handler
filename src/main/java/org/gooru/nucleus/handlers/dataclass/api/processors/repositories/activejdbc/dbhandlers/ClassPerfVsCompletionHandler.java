package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCourseCollectionCount;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


/**
 * Created by mukul@gooru
 * 
 */
public class ClassPerfVsCompletionHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClassPerfVsCompletionHandler.class);
  private final ProcessorContext context;

  ClassPerfVsCompletionHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }


  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
    if (context.getUserIdFromRequest() == null) {
      LOGGER.debug("ClassId is " + this.context.request().getString(MessageConstants.CLASS_ID));
      LOGGER.debug("UserId from session is " + this.context.userIdFromSession());
      List<Map> owner = Base.findAll(AJEntityClassAuthorizedUsers.SELECT_CLASS_OWNER,
          this.context.request().getString(MessageConstants.CLASS_ID),
          this.context.userIdFromSession());
      if (owner.isEmpty()) {
        LOGGER.debug("validateRequest() FAILED");
        return new ExecutionResult<>(
            MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"),
            ExecutionStatus.FAILED);
      }
    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {

    String classId = this.context.request().getString(MessageConstants.CLASS_ID);
    String courseId = this.context.request().getString(MessageConstants.COURSE_ID);
    Integer totalCount;;

    if (classId.isEmpty()) {
      LOGGER.warn("ClassId is mandatory to fetch Student Performance Vs Completion Data");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "ClassId is Missing. Cannot fetch Student Performance Vs Completion Data"),
          ExecutionStatus.FAILED);
    }

    if (courseId.isEmpty()) {
      LOGGER.warn("CourseId is mandatory to fetch Student Performance Vs Completion Data");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "CourseId is Missing. Cannot fetch Student Performance Vs Completion Data"),
          ExecutionStatus.FAILED);
    }

    JsonObject result = new JsonObject();
    JsonArray ClassKpiArray = new JsonArray();

    List<Map> classPerfList;
    List<String> userList = new ArrayList<>();

    Object classTotalCount =
        Base.firstCell(AJEntityCourseCollectionCount.GET_COURSE_ASSESSMENT_COUNT, courseId);
    if (classTotalCount != null) {
      totalCount = Integer.valueOf(classTotalCount.toString());
    } else {
      totalCount = 0;
    }
    LazyList<AJEntityBaseReports> studClass =
        AJEntityBaseReports.findBySQL(AJEntityBaseReports.GET_DISTINCT_USERS_IN_CLASS, classId);
    if (!studClass.isEmpty()) {
      studClass.forEach(users -> userList.add(users.getString(AJEntityBaseReports.GOORUUID)));
      classPerfList =
          Base.findAll(AJEntityBaseReports.SELECT_ALL_STUDENT_CLASS_PERFORMANCE_COMPLETION, classId,
              courseId, listToPostgresArrayString(userList));
      if (!classPerfList.isEmpty()) {
        classPerfList.forEach(scoData -> {
          JsonObject classKPI = new JsonObject();
          classKPI.put(JsonConstants.USERID, scoData.get(AJEntityBaseReports.GOORUUID));
          if (totalCount > 0) {
            classKPI.put(JsonConstants.PERCENT_COMPLETION,
                Math.round((Double.valueOf(
                    scoData.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()) / totalCount)
                    * 100));
          } else {
            classKPI.put(JsonConstants.PERCENT_COMPLETION, 0);
            LOGGER.warn("Total Count of Assessments cannot be obtained");
          }
          classKPI.put(JsonConstants.PERCENT_SCORE,
              scoData.get(AJEntityBaseReports.ATTR_SCORE) == null ? null
                  : Math.round(
                      Double.valueOf(scoData.get(AJEntityBaseReports.ATTR_SCORE).toString())));
          ClassKpiArray.add(classKPI);
        });
      }
    } else {
      LOGGER.info("No Completion Vs Performance Data available");
    }


    // Form the required Json pass it on
    result.put(JsonConstants.USAGE_DATA, ClassKpiArray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);
  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

  private String listToPostgresArrayString(List<String> input) {
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
