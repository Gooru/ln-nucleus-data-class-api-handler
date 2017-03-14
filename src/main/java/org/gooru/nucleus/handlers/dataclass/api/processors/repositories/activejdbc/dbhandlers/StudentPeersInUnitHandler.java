package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
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
 * Created by mukul@gooru
 */

public class StudentPeersInUnitHandler implements DBHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(StudentPeersInUnitHandler.class);
	  private final ProcessorContext context;
    private String classId;
    private String courseId;
    private String unitId;
        
    public StudentPeersInUnitHandler(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public ExecutionResult<MessageResponse> checkSanity() {
    	
    	//No Sanity Check required since, no params are being passed in Request Body
 
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
        JsonObject resultBody = new JsonObject();
        JsonArray StudentPeerArray = new JsonArray();
        this.classId = context.classId();
        this.courseId = context.courseId();
        this.unitId = context.unitId();
        List<Map> UnitPeerMap = Base.findAll(AJEntityBaseReports.GET_PEERS_COUNT_IN_UNIT, this.classId,this.context.userIdFromSession(), this.courseId, this.unitId);
    
        if (!UnitPeerMap.isEmpty()) {
    
          UnitPeerMap.forEach(m -> {
            Integer peerCount = Integer.valueOf(m.get(AJEntityBaseReports.ATTR_PEER_COUNT).toString());
            StudentPeerArray.add(new JsonObject().put(AJEntityBaseReports.ATTR_LESSON_ID, m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString())
                    .put(AJEntityBaseReports.ATTR_PEER_COUNT, (peerCount)));
          });
    
        } else {
          LOGGER.error("Student Peers Count for Unit cannot be obtained. There may be no peers for this Unit");
        }
    
        resultBody.put(JsonConstants.CONTENT, StudentPeerArray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);
        return new ExecutionResult<>(MessageResponseFactory.createGetResponse(resultBody), ExecutionStatus.SUCCESSFUL);
      }   
    

    @Override
    public boolean handlerReadOnly() {
        return false;
    }
	
}
