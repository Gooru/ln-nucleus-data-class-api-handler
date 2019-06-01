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
  private final String studentId;
  private final String questionId;
  private final String startDate;
  private final String endDate;
  private final String collectionType;
  private final String milestoneId;
  private Boolean isInternal;
  private final String studId;
  private final String oaId;
  private final String itemId;

  public ProcessorContext(JsonObject request, String userIdFromSession, String userIdFromRequest,
      String classId, String courseId, String unitId, String lessonId, String collectionId,
      String sessionId, String studentId, String questionId, String startDate, String endDate,
      String collectionType, String milestoneId, String studId, String oaId, String itemId) {
    this.request = request != null ? request.copy() : null;
    this.userIdFromSession = userIdFromSession;
    this.userIdFromRequest = userIdFromRequest;
    this.classId = classId;
    this.courseId = courseId;
    this.unitId = unitId;
    this.lessonId = lessonId;
    this.collectionId = collectionId;
    this.sessionId = sessionId;
    this.studentId = studentId;
    this.questionId = questionId;
    this.startDate = startDate;
    this.endDate = endDate;
    this.collectionType = collectionType;
    this.milestoneId = milestoneId;
    this.isInternal = false;
    this.studId = studId;
    this.oaId = oaId;
    this.itemId = itemId;
  }

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

  public String collectionType() {
    return collectionType;
  }

  public String getUserIdFromRequest() {
    return userIdFromRequest;
  }

  public String studentId() {
    return studentId;
  }

  public String questionId() {
    return questionId;
  }

  public String startDate() {
    return startDate;
  }

  public String endDate() {
    return endDate;
  }

  public String milestoneId() {
    return milestoneId;
  }

  public Boolean isInternal() {
    return this.isInternal;
  }

  public void setIsIntenal(Boolean isInternal) {
    this.isInternal = isInternal;
  }
  
  public String studId() {
    return studId;
  }
  
  public String oaId() {
    return oaId;
  }
  
  public String itemId() {
    return itemId;
  }
}
