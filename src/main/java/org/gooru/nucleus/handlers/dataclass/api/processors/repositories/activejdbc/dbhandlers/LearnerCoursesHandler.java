package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityContent;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityUserTaxonomySubject;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author daniel
 */

public class LearnerCoursesHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(LearnerCoursesHandler.class);
  private final ProcessorContext context;
  private String userId;
  private String nullVal = null;

  public LearnerCoursesHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    if (context.request() == null || context.request().isEmpty()) {
      LOGGER.warn("invalid request received to fetch learner courses");
      return new ExecutionResult<>(MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch independent learner courses"),
              ExecutionStatus.FAILED);
    }

    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> validateRequest() {
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {
    JsonObject resultBody = new JsonObject();
    JsonArray resultarray = new JsonArray();
    this.userId = this.context.userIdFromSession();
    LOGGER.debug("UID is " + this.userId);
    String taxSubjectId = this.context.request().getString("taxSubjectId");
    String userType = this.context.request().getString("userType");
    List<Map> coursesList = null;
    if (taxSubjectId == null || (taxSubjectId != null && (taxSubjectId.equalsIgnoreCase("all") || taxSubjectId.equals("*")))) {
      // TODO : IL represents IndependentLearner. This can be changed later.
      if (userType != null && userType.equalsIgnoreCase("IL")) {
        coursesList = Base.findAll(AJEntityUserTaxonomySubject.GET_INDEPENDENT_LEARNER_ALL_COURSES, this.userId);
      } else {
        coursesList = Base.findAll(AJEntityUserTaxonomySubject.GET_LEARNER_ALL_COURSES, this.userId);
      }
    } else {
      if (userType != null && userType.equalsIgnoreCase("IL")) {
        coursesList = Base.findAll(AJEntityUserTaxonomySubject.GET_INDEPENDENT_LEARNER_COURSES, taxSubjectId, this.userId);

      } else {
        coursesList = Base.findAll(AJEntityUserTaxonomySubject.GET_LEARNER_COURSES, taxSubjectId, this.userId);
      }
    }
    if (!coursesList.isEmpty()) {
      coursesList.forEach(course -> {
        JsonObject contentBody = new JsonObject();
        contentBody.put(AJEntityBaseReports.ATTR_COURSE_ID, course.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
        Object title = Base.firstCell(AJEntityContent.GET_TITLE, course.get(AJEntityBaseReports.COURSE_GOORU_OID).toString());
        contentBody.put(AJEntityContent.ATTR_COURSE_TITLE, title);
        if (course.get(AJEntityBaseReports.CLASS_GOORU_OID) != null) {
          contentBody.put(AJEntityBaseReports.ATTR_CLASS_ID, course.get(AJEntityBaseReports.CLASS_GOORU_OID).toString());
          List<Map> classInfo = Base.findAll(AJEntityContent.GET_CLASS_TITLE_CODE, course.get(AJEntityBaseReports.CLASS_GOORU_OID).toString());
          classInfo.forEach(classData -> {
            contentBody.put(AJEntityContent.ATTR_CLASS_CODE, AJEntityContent.CLASS_CODE);
            contentBody.put(AJEntityContent.ATTR_CLASS_TITLE, AJEntityContent.TITLE);

          });
        } else {
          contentBody.put(AJEntityBaseReports.ATTR_CLASS_ID, nullVal);
          contentBody.put(AJEntityContent.ATTR_CLASS_CODE, nullVal);
          contentBody.put(AJEntityContent.ATTR_CLASS_TITLE, nullVal);
        }
        resultarray.add(contentBody);
      });
    } else {
      // Return an empty resultBody instead of an Error
      LOGGER.debug("No data returned for learner courses");
    }
    resultBody.put(JsonConstants.CONTENT, resultarray);

    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return false;
  }
}
