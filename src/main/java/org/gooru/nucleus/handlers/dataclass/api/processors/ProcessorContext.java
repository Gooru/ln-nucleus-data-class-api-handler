package org.gooru.nucleus.handlers.dataclass.api.processors;

import io.vertx.core.json.JsonObject;

/**
 * Created by mukul@gooru
 */
public class ProcessorContext {

	  private final String userIdFromSession;   
	  private final String userIdFromRequest;    
    private final JsonObject request;
    private final String classId;
    private final String courseId;
    private final String unitId;
    private final String lessonId;
    private final String collectionId;
    private final String sessionId;

    public ProcessorContext(JsonObject request, String userIdFromSession, String userIdFromRequest, String classId, String courseId, String unitId, String lessonId, String collectionId, String sessionId) {        
        this.request = request != null ? request.copy() : null;
        this.userIdFromSession = userIdFromSession;
        this.userIdFromRequest = userIdFromRequest;
        this.classId = classId;
        this.courseId = courseId;
        this.unitId = unitId;
        this.lessonId = lessonId;
        this.collectionId = collectionId;
        this.sessionId = sessionId;
    }

    //Mukul - TODO 
    //Sort out User Auth
    public String userIdFromSession() {
        return this.userIdFromSession;
    }

    public JsonObject request() {
        return this.request;
    }

    public String classId() {
        return this.classId;
    }
    
    public String courseId() {
        return this.courseId;
    }

    public String unitId() {
        return this.unitId;
    }

    public String lessonId() {
        return this.lessonId;
    }

    public String collectionId() {
        return this.collectionId;
    }

    public String sessionId() {
        return this.sessionId;
    }

    public String getUserIdFromRequest() {
      return userIdFromRequest;
    }
}
