package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
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
 * Created by mukul@gooru
 */

public class StudentPeersInUnitHandler implements DBHandler {


	private static final Logger LOGGER = LoggerFactory.getLogger(StudentPeersInUnitHandler.class);
    
	private final ProcessorContext context;
    private AJEntityBaseReports baseReport;

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
    public ExecutionResult<MessageResponse> executeRequest() {
    	
    	JsonObject resultBody = new JsonObject();
    	JsonArray StudentPeerArray = new JsonArray();

    	baseReport = new AJEntityBaseReports();
    
        this.classId = context.classId();
        this.courseId = context.courseId();
        this.unitId = context.unitId();
        
    	List<Map> UnitPeerMap = Base.findAll( AJEntityBaseReports.GET_STUDENT_PEERS_IN_UNIT, this.classId, this.courseId, this.unitId);
    	
    	if (!UnitPeerMap.isEmpty()){
    		
             UnitPeerMap.forEach(m -> {
            	 Integer peerCount = Integer.valueOf(m.get(AJEntityBaseReports.ATTR_PEER_COUNT).toString());            
            	 StudentPeerArray.add(new JsonObject().put(AJEntityBaseReports.LESSON_GOORU_OID, m.get(AJEntityBaseReports.LESSON_GOORU_OID).toString())
            	    		.put(AJEntityBaseReports.ATTR_PEER_COUNT, (peerCount -1)));
             });             
             
    	} else {
            LOGGER.error("Student Peers Count for Unit cannot be obtained");
            return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
        }

    	resultBody.put(JsonConstants.CONTENT, StudentPeerArray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);
                 
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
