package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;

/**
 * Created by mukul@gooru
 */

public final class DBHandlerBuilder {

    private DBHandlerBuilder() {
        throw new AssertionError();
    }

    public static DBHandler buildStudentPeersInCourseHandler(ProcessorContext context) {
        return new StudentPeersInCourseHandler(context);
    }

    public static DBHandler buildStudentPeersInUnitHandler(ProcessorContext context) {
        return new StudentPeersInUnitHandler(context);
    }
    
    public static DBHandler buildStudentPeersInLessonHandler(ProcessorContext context) {
        return new StudentPeersInLessonHandler(context);
    }

    public static DBHandler buildStudentCurrentLocationHandler(ProcessorContext context) {
        return new StudentCurrentLocationHandler(context);
    }
    
    public static DBHandler buildStudentCoursePerfHandler(ProcessorContext context) {
        return new StudentCoursePerfHandler(context);
    }
    
    public static DBHandler buildStudentUnitPerfHandler(ProcessorContext context) {
        return new StudentUnitPerfHandler(context);
    }
    
    public static DBHandler buildStudentLessonPerfHandler(ProcessorContext context) {
        return new StudentLessonPerfHandler(context);
    }
    
    public static DBHandler buildStudentCollectionPerfHandler(ProcessorContext context) {
        return new StudentCollectionPerfHandler(context);
    }
    
    public static DBHandler buildStudentAssessmentPerfHandler(ProcessorContext context) {
        return new StudentAssessmentPerfHandler(context);
    }
    
    public static DBHandler buildSessionStatusHandler(ProcessorContext context) {
        return new SessionStatusHandler(context);
    }
    
    public static DBHandler buildUserAssessmentSessionsHandler(ProcessorContext context) {
        return new UserAssessmentSessionsHandler(context);
    }
    
    public static DBHandler buildUserCollectionSessionsHandler(ProcessorContext context) {
        return new UserCollectionSessionsHandler(context);
    }
    
    public static DBHandler buildAllStudentCoursePerfHandler(ProcessorContext context) {
        return new AllStudentCoursePerfHandler(context);
    }
    
    public static DBHandler buildAllStudentUnitPerfHandler(ProcessorContext context) {
        return new AllStudentUnitPerfHandler(context);
    }
}
