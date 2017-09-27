package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.Iterator;
import java.util.List;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;

import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */
public class AllStudentCoursePerfHandler implements DBHandler {


	private static final Logger LOGGER = LoggerFactory.getLogger(AllStudentCoursePerfHandler.class);
	private static final String REQUEST_COLLECTION_TYPE = "collectionType";
    private static final String REQUEST_USERID = "userUid";

	private final ProcessorContext context;
    private AJEntityBaseReports baseReport;


    private String collectionType;
    private String userId;

    //For stuffing Json
    private String unitId;
    private String collId;
    private String qtype;
    private String react;
    private String resourceTS;
    private String ansObj;
    private String resType;
    private String resAttemptStatus;
    private String sco;
    private String SID;
    private String resViews;
    private String compCount;

    public AllStudentCoursePerfHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
        if (context.request() == null || context.request().isEmpty()) {
            LOGGER.warn("invalid request received to fetch Student Performance in Course");
            return new ExecutionResult<>(
                MessageResponseFactory.createInvalidRequestResponse("Invalid data provided to fetch Student Performance in course"),
                ExecutionStatus.FAILED);
        }

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

    	JsonObject resultBody = new JsonObject();
        resultBody.put("Message", "CODE TO BE UPDATED AFTER ROLES MODULE IS CREATED");

    	return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody),
                ExecutionStatus.SUCCESSFUL);

    }


    @Override
    public boolean handlerReadOnly() {
        return false;
    }


    private String listToPostgresArrayString(List<String> input) {
        int approxSize = ((input.size() + 1) * 36); // Length of UUID is around
                                                    // 36
                                                    // chars
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
