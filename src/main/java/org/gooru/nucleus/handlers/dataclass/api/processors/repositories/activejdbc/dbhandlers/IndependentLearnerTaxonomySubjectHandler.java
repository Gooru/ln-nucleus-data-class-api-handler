package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityUserTaxonomySubject;
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

/**
 * 
 * @author daniel
 *
 */
public class IndependentLearnerTaxonomySubjectHandler implements DBHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndependentLearnerTaxonomySubjectHandler.class);

  private final ProcessorContext context;

  private String userId;

  private static final String REQUEST_USERID = "userId";

  public IndependentLearnerTaxonomySubjectHandler(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public ExecutionResult<MessageResponse> checkSanity() {
    // No Sanity Check required at this point since, no params are being passed
    // in Request
    LOGGER.debug("checkSanity() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  public ExecutionResult<MessageResponse> validateRequest() {
    LOGGER.debug("validateRequest() OK");
    return new ExecutionResult<>(null, ExecutionStatus.CONTINUE_PROCESSING);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExecutionResult<MessageResponse> executeRequest() {

    this.userId = this.context.request().getString(REQUEST_USERID);
    String userType = this.context.request().getString("userType");
    
    if (StringUtil.isNullOrEmpty(userId)) {
      this.userId = this.context.userIdFromSession();
    }

    JsonObject result = new JsonObject();
    JsonArray subjectArray = new JsonArray();

    List<Map> taxSubjects = null;
    if(!StringUtil.isNullOrEmpty(userType) && userType.equalsIgnoreCase("IL")){      
      taxSubjects = Base.findAll(AJEntityUserTaxonomySubject.GET_IL_TAX_SUBJECTS, this.userId);
    }else{
      taxSubjects = Base.findAll(AJEntityUserTaxonomySubject.GET_LEARNER_TAX_SUBJECTS, this.userId);      
    }
    if (!taxSubjects.isEmpty()) {
      taxSubjects.forEach(m -> {
        JsonObject subjectsInfo = new JsonObject();
        subjectsInfo.put(AJEntityUserTaxonomySubject.ATTR_TAX_SUBJECT_ID, m.get(AJEntityUserTaxonomySubject.TAX_SUBJECT_ID).toString());
        Object title =
                Base.firstCell(AJEntityUserTaxonomySubject.GET_SUBJECT_TITLE, m.get(AJEntityUserTaxonomySubject.TAX_SUBJECT_ID).toString());
        subjectsInfo.put(AJEntityUserTaxonomySubject.ATTR_TAX_SUBJECT_TITLE, title);
        subjectArray.add(subjectsInfo);
      });
    } else {
      LOGGER.info("Taxonomy Subject for the Independent Learner cannot be obtained");
    }
    result.put(JsonConstants.USAGE_DATA, subjectArray).put(JsonConstants.USERID, this.userId);
    return new ExecutionResult<>(MessageResponseFactory.createGetResponse(result), ExecutionStatus.SUCCESSFUL);

  }

  @Override
  public boolean handlerReadOnly() {
    return true;
  }

}
