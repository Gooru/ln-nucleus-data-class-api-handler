package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.handlers.dataclass.api.constants.EventConstants;
import org.gooru.nucleus.handlers.dataclass.api.constants.JsonConstants;
import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityBaseReports;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponseFactory;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.ExecutionResult.ExecutionStatus;
import org.javalite.activejdbc.Base;
import org.javalite.activejdbc.LazyList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */

public class StudentPeersInLessonHandler implements DBHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(StudentPeersInUnitHandler.class);
    
	private final ProcessorContext context;
    private AJEntityBaseReports baseReport;

    private String classId;
    private String courseId;
    private String unitId;
    private String lessonId;
    
    private static final long timeDiff = 900000; 
    
	String cId = new String();
	String user = new String();
	Integer activeUser = 0; 
	Integer inactiveUser = 0;
        
    public StudentPeersInLessonHandler(ProcessorContext context) {
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
    	JsonArray studentPeerArray = new JsonArray();
    	
    	baseReport = new AJEntityBaseReports();
    
        this.classId = context.classId();
        this.courseId = context.courseId();
        this.unitId = context.unitId();
        this.lessonId = context.lessonId();

        resultBody.put(JsonConstants.CONTENT, "WORK IN PROGRESS");
        
        List<String> collIds = new ArrayList<>();
        JsonArray peerArray = new JsonArray();
        
        
        //If CollectionType is Assessment        
        	LazyList<AJEntityBaseReports> collIDforAssessment = AJEntityBaseReports.findBySQL( AJEntityBaseReports.GET_DISTINCT_COLLID_FOR_LESSONID_FILTERBY_COLLTYPE,
                    context.classId(), context.courseId(), context.unitId(), context.lessonId(), EventConstants.ASSESSMENT);
        	
        	if (!collIDforAssessment.isEmpty()) {        		
        		LOGGER.debug("Got a list of Distinct collectionIDs for this lesson");
            	                  
            	collIDforAssessment.forEach(coll -> collIds.add(coll.getString(AJEntityBaseReports.COLLECTION_OID)));
                
                for (String assessmentId : collIds) {
                	//For this CollectionID get Distinct Users
                    List<Map> distinctUsersMap = Base.findAll(AJEntityBaseReports.GET_DISTINCT_USERS_FOR_COLLECTION_FILTERBY_COLLTYPE,
                    		assessmentId, EventConstants.ASSESSMENT);
                    
                    if(distinctUsersMap.isEmpty()){
                    	LOGGER.debug("No data returned for users for this collection");
                    	//return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
                    	continue;
                    }

                    distinctUsersMap.forEach(m -> {
                    	cId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
                        user = m.get(AJEntityBaseReports.ATTR_USERS).toString();
                        LOGGER.debug("the User is " + user);
                    	LOGGER.debug("The Value of CollectionID, should be same as assessmentId " + cId);                    	
                    	List<Map> timestampMap = Base.findAll(AJEntityBaseReports.GET_USERS_LATEST_TIMESTAMP,
                        		context.lessonId(), assessmentId, EventConstants.ASSESSMENT, user);
                    	timestampMap.forEach(ts -> {
                    		Timestamp currTs = new Timestamp(System.currentTimeMillis());
                    		LOGGER.debug(currTs.toString());
                    		Timestamp userTs = Timestamp.valueOf(ts.get(AJEntityBaseReports.UPDATE_TIMESTAMP).toString());
                    		LOGGER.debug(userTs.toString());
                    		
                    		if (((currTs.getTime() - userTs.getTime()) < timeDiff)){
                    			peerArray.add(new JsonObject().put(JsonConstants.ASSESSMENTID, assessmentId).put(JsonConstants.USERUID, user)
                    					.put(JsonConstants.STATUS, JsonConstants.ACTIVE));                        		
                        	} else if (((currTs.getTime() - userTs.getTime()) > timeDiff)){
                        		peerArray.add(new JsonObject().put(JsonConstants.ASSESSMENTID, assessmentId).put(JsonConstants.USERUID, user)
                    					.put(JsonConstants.STATUS, JsonConstants.INACTIVE));                        		
                        	}                    		
                    	});
                	});
                } 
        	}

            //If CollectionType is Collection 
        	LazyList<AJEntityBaseReports> collIDforCollection = AJEntityBaseReports.findBySQL( AJEntityBaseReports.GET_DISTINCT_COLLID_FOR_LESSONID_FILTERBY_COLLTYPE,
                    context.classId(), context.courseId(), context.unitId(), context.lessonId(), EventConstants.COLLECTION);
        	
        	if (!collIDforCollection.isEmpty()) {        		
        		LOGGER.debug("Got a list of Distinct collectionIDs for this lesson");
            	                  
        		collIDforCollection.forEach(coll -> collIds.add(coll.getString(AJEntityBaseReports.COLLECTION_OID)));
                
                for (String collectionId : collIds) {
                	//For this CollectionID get Distinct Users
                    List<Map> distinctUsersMap = Base.findAll(AJEntityBaseReports.GET_DISTINCT_USERS_FOR_COLLECTION_FILTERBY_COLLTYPE,
                    		collectionId, EventConstants.COLLECTION);
                    
                    if(distinctUsersMap.isEmpty()){
                    	LOGGER.debug("No data returned for users for this collection");
                    	//return new ExecutionResult<>(MessageResponseFactory.createNotFoundResponse(), ExecutionStatus.FAILED);
                    	continue;
                    }

                    distinctUsersMap.forEach(m -> {
                    	cId = m.get(AJEntityBaseReports.COLLECTION_OID).toString();
                        user = m.get(AJEntityBaseReports.ATTR_USERS).toString();
                    	LOGGER.debug("The Value of CollectionID, should be same as collectionId " + cId);                    	
                    	List<Map> timestampMap = Base.findAll(AJEntityBaseReports.GET_USERS_LATEST_TIMESTAMP,
                        		context.lessonId(), collectionId, EventConstants.COLLECTION, user);
                    	timestampMap.forEach(ts -> {
                    		Timestamp currTs = new Timestamp(System.currentTimeMillis());
                    		Timestamp userTs = Timestamp.valueOf(ts.get(AJEntityBaseReports.UPDATE_TIMESTAMP).toString());
                    		
                    		if (((currTs.getTime() - userTs.getTime()) < timeDiff)){
                    			peerArray.add(new JsonObject().put(JsonConstants.COLLECTIONID, collectionId).put(JsonConstants.USERUID, user)
                    					.put(JsonConstants.STATUS, JsonConstants.ACTIVE));                        		
                        	} else if (((currTs.getTime() - userTs.getTime()) > timeDiff)){
                        		peerArray.add(new JsonObject().put(JsonConstants.COLLECTIONID, collectionId).put(JsonConstants.USERUID, user)
                    					.put(JsonConstants.STATUS, JsonConstants.INACTIVE));                        		
                        	}                    		
                    	});
                	});
                } 
        	}

        	resultBody.put(JsonConstants.CONTENT, peerArray).putNull(JsonConstants.MESSAGE).putNull(JsonConstants.PAGINATE);
                 
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
