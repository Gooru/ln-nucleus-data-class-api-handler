package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.util.StringUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author mukul@gooru
 * 
 */
public class StudDCAAllClassPerfHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(StudDCAAllClassPerfHandler.class);
  private final ProcessorContext context;
  private static final String REQUEST_USERID = "userId";

  private String userId;
  private List<String> reqClassIds = new ArrayList<>();
  private List cIds = new ArrayList<>();
  private List<String> classIds = new ArrayList<>();

  StudDCAAllClassPerfHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> checkSanity() {
    this.userId = this.context.request().getString(REQUEST_USERID);

    JsonArray classes = this.context.request().getJsonArray(EventConstants.CLASS_IDS);
    LOGGER.debug("userId : {} - classIds:{}", this.userId, classes);

    if (classes.isEmpty()) {
      LOGGER.warn("ClassIds are mandatory to fetch Student Performance in Classess");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse(
          "ClassIds missing. Cannot fetch performance in classes"), ExecutionStatus.FAILED);
    }

    for (Object s : classes) {
      LOGGER.debug("The Request Class String is " + s.toString());
      this.reqClassIds.add(s.toString());
    }

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
    if (!this.context.isInternal()) {
      if (context.getUserIdFromRequest() == null || (context.getUserIdFromRequest() != null
          && !context.userIdFromSession().equalsIgnoreCase(this.context.getUserIdFromRequest()))) {
        cIds = Base.firstColumn(AJEntityClassAuthorizedUsers.SELECT_CLASSES,
            listToPostgresArrayString(this.reqClassIds), this.context.userIdFromSession());
        if (cIds == null || cIds.isEmpty()) {
          LOGGER.debug("validateRequest() FAILED");
          return new ExecutionResult<>(
              MessageResponseFactory.createForbiddenResponse("User is not a teacher/collaborator"),
              ExecutionStatus.FAILED);
        }
      }
    } else {
      cIds.addAll(reqClassIds);
    }
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    JsonArray classKpiArray;

    if (!StringUtil.isNullOrEmpty(this.userId)) {
      // Student access to Class Data
      classKpiArray = fetchClassKPIForStudent();
    } else {
      // Teacher access to Class Data
      if (cIds != null && !cIds.isEmpty()) {
        for (Object c : cIds) {
          LOGGER.debug("The Class String is " + c.toString());
          this.classIds.add(c.toString());
        }
      }
      classKpiArray = fetchClassKPIForTeacher();
    }

    result.put(JsonConstants.USAGE_DATA, classKpiArray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);
  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

  private JsonArray fetchClassKPIForStudent() {
    JsonArray classKpiArray = new JsonArray();
    // Average performance across All attempts of Assessments /
    // Ext-Assessments for class
    LOGGER.debug("Inside Student");
    List<Map> classPerfList =
        Base.findAll(AJEntityDailyClassActivity.SELECT_STUDENT_CLASSES_COMPLETION_SCORE,
            listToPostgresArrayString(this.reqClassIds), this.userId);

    if (!classPerfList.isEmpty()) {
      classPerfList.forEach(scoData -> {
        JsonObject classKPI = new JsonObject();
        classKPI.put(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT,
            scoData.get(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT) == null ? 0
                : Integer.valueOf(
                    scoData.get(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT).toString()));
        classKPI.put(AJEntityDailyClassActivity.ATTR_SCORE,
            scoData.get(AJEntityDailyClassActivity.ATTR_SCORE) == null ? null
                : Math.round(
                    Double.valueOf(scoData.get(AJEntityDailyClassActivity.ATTR_SCORE).toString())));
        classKPI.put(AJEntityDailyClassActivity.ATTR_CLASS_ID,
            scoData.get(AJEntityDailyClassActivity.CLASS_GOORU_OID).toString());
        classKpiArray.add(classKPI);
      });
    }

    return classKpiArray;
  }

  private JsonArray fetchClassKPIForTeacher() {
    JsonArray classKpiArray = new JsonArray();
    // Average performance across All attempts of Assessments /
    // Ext-Assessments
    List<Map> classPerfList =
        Base.findAll(AJEntityDailyClassActivity.SELECT_CLASS_COMPLETION_SCORE_FOR_TEACHER,
            listToPostgresArrayString(this.classIds));

    if (!classPerfList.isEmpty()) {
      classPerfList.forEach(scoData -> {
        JsonObject classKPI = new JsonObject();
        classKPI.put(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT,
            scoData.get(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT) == null ? 0
                : Integer.valueOf(
                    scoData.get(AJEntityDailyClassActivity.ATTR_COMPLETED_COUNT).toString()));
        classKPI.put(AJEntityDailyClassActivity.ATTR_SCORE,
            scoData.get(AJEntityDailyClassActivity.ATTR_SCORE) == null ? null
                : Math.round(
                    Double.valueOf(scoData.get(AJEntityDailyClassActivity.ATTR_SCORE).toString())));
        classKPI.put(AJEntityDailyClassActivity.ATTR_CLASS_ID,
            scoData.get(AJEntityDailyClassActivity.CLASS_GOORU_OID).toString());
        classKpiArray.add(classKPI);
      });
    }

    return classKpiArray;
  }

  private String listToPostgresArrayString(List<String> input) {
    int approxSize = ((input.size() + 1) * 36); // Length of UUID is around
                                                // 36 chars
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
