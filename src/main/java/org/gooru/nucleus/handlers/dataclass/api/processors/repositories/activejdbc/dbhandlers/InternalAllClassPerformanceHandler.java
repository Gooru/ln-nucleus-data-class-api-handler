
package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassMember;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCourseCollectionCount;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.utils.PgUtils;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author szgooru Created On 12-Apr-2019
 */
public class InternalAllClassPerformanceHandler implements DBHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(InternalAllClassPerformanceHandler.class);

  private final ProcessorContext context;

  public InternalAllClassPerformanceHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonArray classes = this.context.request().getJsonArray(EventConstants.CLASSES);

    LOGGER.debug("classes:{}", classes);

    if (classes == null || classes.isEmpty()) {
      LOGGER.warn("Classes array is mandatory to fetch Student Performance in Classes");
      return new ExecutionResult<>(
          MessageResponseFactory.createInvalidRequestResponse(
              "Classes array is Missing. Cannot fetch Student Performance in Classes"),
          ExecutionStatus.FAILED);
    }

    List<Map<String, String>> classList = new ArrayList<>(classes.size());
    for (Object cls : classes) {
      JsonObject classObject = (JsonObject) cls;
      if ((classObject.containsKey(EventConstants.CLASS_ID)
          && !StringUtil.isNullOrEmpty(classObject.getString(EventConstants.CLASS_ID)))
          && classObject.containsKey(EventConstants.COURSE_ID)
          && !StringUtil.isNullOrEmpty(classObject.getString(EventConstants.COURSE_ID))) {
        Map<String, String> classObj = new HashMap<>();
        classObj.put(EventConstants.CLASS_ID, classObject.getString(EventConstants.CLASS_ID));
        classObj.put(EventConstants.COURSE_ID, classObject.getString(EventConstants.COURSE_ID));
        classList.add(classObj);
      }
    }
    if (classList.isEmpty()) {
      LOGGER.warn("ClassIds and courseIds are mandatory to fetch Student Performance in Classes");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "Both or either of classId or courseId is missing. Cannot fetch Student Performance in Classes"),
          ExecutionStatus.FAILED);
    }

    JsonObject result = new JsonObject();
    JsonArray classKpiArray = new JsonArray();


    for (Map<String, String> classMap : classList) {
      String clId = classMap.get(EventConstants.CLASS_ID);
      String courseId = classMap.get(EventConstants.COURSE_ID);
      if (!StringUtil.isNullOrEmpty(courseId)) {
        JsonObject classKPI = new JsonObject();
        Object activeUsersCountObj =
            Base.firstCell(AJEntityClassMember.SELECT_ACTIVE_USERS_COUNT, clId);
        // FIXME: It should not be null or 0. If activeUsersCount is null or 0, look at sync job
        // is working..
        int activeUsersCount =
            activeUsersCountObj == null ? 0 : Integer.valueOf(activeUsersCountObj.toString());
        List<Map> classPerfData =
            Base.findAll(AJEntityBaseReports.SELECT_ALL_STUDENTS_CLASS_DATA, clId, courseId);
        if (!classPerfData.isEmpty()) {
          classPerfData.forEach(classData -> {
            classKPI.put(AJEntityBaseReports.ATTR_CLASS_ID, clId);
            classKPI.put(AJEntityBaseReports.ATTR_TIME_SPENT,
                classData.get(AJEntityBaseReports.ATTR_TIME_SPENT) == null ? 0
                    : Long.valueOf(classData.get(AJEntityBaseReports.ATTR_TIME_SPENT).toString()));
            Object classTotalCount =
                Base.firstCell(AJEntityCourseCollectionCount.GET_COURSE_ASSESSMENT_COUNT, courseId);
            classKPI.put(AJEntityBaseReports.ATTR_TOTAL_COUNT,
                classTotalCount != null
                    ? (Integer.valueOf(classTotalCount.toString()) * activeUsersCount)
                    : 0);
            // classKPI.put(AJEntityBaseReports.ATTR_TOTAL_COUNT, 0);
          });
          List<String> userList = new ArrayList<>();
          LazyList<AJEntityBaseReports> studClass =
              AJEntityBaseReports.findBySQL(AJEntityBaseReports.GET_DISTINCT_USERS_IN_CLASS, clId);
          studClass.forEach(users -> userList.add(users.getString(AJEntityBaseReports.GOORUUID)));

          List<Map> classPerfList =
              Base.findAll(AJEntityBaseReports.SELECT_ALL_STUDENT_CLASS_COMPLETION_SCORE, clId,
                  courseId, PgUtils.listToPostgresArrayString(userList));

          if (!classPerfList.isEmpty()) {
            classPerfList.forEach(scoData -> {
              classKPI.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT,
                  scoData.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT) == null ? 0
                      : Integer.valueOf(
                          scoData.get(AJEntityBaseReports.ATTR_COMPLETED_COUNT).toString()));
              classKPI.put(AJEntityBaseReports.ATTR_SCORE,
                  scoData.get(AJEntityBaseReports.ATTR_SCORE) == null ? null
                      : Math.round(
                          Double.valueOf(scoData.get(AJEntityBaseReports.ATTR_SCORE).toString())));
            });
          } else {
            classKPI.put(AJEntityBaseReports.ATTR_COMPLETED_COUNT, 0);
            classKPI.putNull(AJEntityBaseReports.ATTR_SCORE);
          }

        }
        classKpiArray.add(classKPI);
      }
    }

    // Form the required Json pass it on
    result.put(JsonConstants.USAGE_DATA, classKpiArray);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);
  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }// TODO Auto-generated constructor stub

}
