
package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityClassAuthorizedUsers;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityDailyClassActivity;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.utils.PgUtils;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author szgooru Created On 12-Apr-2019
 */
public class InternalAllClassDCAPerformanceHandler implements DBHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(InternalAllClassDCAPerformanceHandler.class);
  private final ProcessorContext context;

  private List<String> reqClassIds = new ArrayList<>();
  private List cIds = new ArrayList<>();
  private List<String> classIds = new ArrayList<>();

  public InternalAllClassDCAPerformanceHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    JsonArray classes = this.context.request().getJsonArray(EventConstants.CLASS_IDS);
    LOGGER.debug("classIds:{}", classes);

    if (classes.isEmpty()) {
      LOGGER.warn("ClassIds are mandatory to fetch Performance in Classess");
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
  public ExecutionResult<MessageResponse> validateRequest() {
    cIds = Base.firstColumn(AJEntityClassAuthorizedUsers.SELECT_CLASSES_FOR_INTERNAL_API,
        PgUtils.listToPostgresArrayString(this.reqClassIds));
    if (cIds == null || cIds.isEmpty()) {
      LOGGER.debug("validateRequest() FAILED");
      return new ExecutionResult<>(
          MessageResponseFactory.createNotFoundResponse("classes not found"),
          ExecutionStatus.FAILED);
    }

    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject result = new JsonObject();
    JsonArray classKpiArray;

    for (Object c : cIds) {
      LOGGER.debug("The Class String is " + c.toString());
      this.classIds.add(c.toString());
    }
    classKpiArray = fetchClassKPIForTeacher();
    result.put(JsonConstants.USAGE_DATA, classKpiArray);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result),
        ExecutionStatus.SUCCESSFUL);
  }

  private JsonArray fetchClassKPIForTeacher() {
    JsonArray classKpiArray = new JsonArray();
    // Average performance across All attempts of Assessments /
    // Ext-Assessments
    List<Map> classPerfList =
        Base.findAll(AJEntityDailyClassActivity.SELECT_CLASS_COMPLETION_SCORE_FOR_TEACHER,
            PgUtils.listToPostgresArrayString(this.classIds));

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

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
