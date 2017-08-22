package org.gooru.nucleus.handlers.dataclass.api.constants;

public final class MessageConstants {

    public static final String MSG_HEADER_OP = "mb.operation";
    public static final String MSG_HEADER_TOKEN = "session.token";
    public static final String MSG_OP_AUTH_WITH_PREFS = "auth.with.prefs";
    public static final String MSG_OP_STATUS = "mb.operation.status";
    public static final String MSG_KEY_PREFS = "prefs";
    public static final String MSG_OP_STATUS_SUCCESS = "success";
    public static final String MSG_OP_STATUS_ERROR = "error";
    public static final String MSG_OP_STATUS_VALIDATION_ERROR = "error.validation";
    public static final String MSG_USER_ANONYMOUS = "anonymous";
    public static final String MSG_USER_ID = "user_id";
    public static final String MSG_HTTP_STATUS = "http.status";
    public static final String MSG_HTTP_BODY = "http.body";
    public static final String MSG_HTTP_RESPONSE = "http.response";
    public static final String MSG_HTTP_ERROR = "http.error";
    public static final String MSG_HTTP_VALIDATION_ERROR = "http.validation.error";
    public static final String MSG_HTTP_HEADERS = "http.headers";
    public static final String MSG_MESSAGE = "message";

    // Operation names: Also need to be updated in corresponding handlers
    public static final String MSG_OP_CONTENT_PUBLISH = "content.publish";
    public static final String MSG_OP_CONTENT_FLAG = "content.flag";
    public static final String MSG_OP_CONTENT_PUBLISH_STATUS = "content.publish.status";
    public static final String MSG_OP_REQUEST_CONTENT_PUBLISH = "content.publish.request";

    // Containers for different responses
    public static final String RESP_CONTAINER_MBUS = "mb.container";
    public static final String RESP_CONTAINER_EVENT = "mb.event";

    public static final String ASSESSMENT_ID = "assessmentId";
    public static final String ID = "id";
    public static final String CONTENT_ID = "contentId";
    
    public static final String USER_ID = "userId";
    public static final String _USER_ID = "user_id";
    public static final String USER_UID = "userUid";
    public static final String CLASS_ID = "classId";
    public static final String COURSE_ID = "courseId";
    public static final String COURSE_IDS = "courseIds";
    public static final String UNIT_ID = "unitId";
    public static final String LESSON_ID = "lessonId";
    public static final String COLLECTION_ID = "collectionId";
    public static final String COLLECTION_IDS = "collectionIds";
    public static final String SESSION_ID = "sessionId";
    public static final String RESP_JSON_KEY_RESOURCES = "resources";
    public static final String START_DATE = "startDate";
    public static final String END_DATE = "endDate";
    public static final String COURSE = "course";
    public static final String ASSESSMENT = "assessment";
    public static final String COLLECTION = "collection";
    //Rubric Grading
    public static final String STUDENT_ID = "student_id";
    public static final String STUDENTID = "studentId";
    public static final String QUESTION_ID = "question_id";
    
    
    // Read API Constants
    public static final String MSG_OP_COURSE_PEERS = "student.peers.in.course";    
    public static final String MSG_OP_UNIT_PEERS = "student.peers.in.unit";
    public static final String MSG_OP_LESSON_PEERS = "student.peers.in.lesson";    
    public static final String MSG_OP_STUDENT_CURRENT_LOC = "student.current.loc";
    public static final String MSG_OP_STUDENT_COURSE_PERF = "student.course.performance";
    public static final String MSG_OP_STUDENT_UNIT_PERF = "student.unit.performance";
    public static final String MSG_OP_STUDENT_LESSON_PERF = "student.lesson.performance";
    public static final String MSG_OP_STUDENT_COLLECTION_SUMMARY = "student.collection.summary";
    public static final String MSG_OP_STUDENT_ASSESSMENT_SUMMARY = "student.assessment.summary";
    public static final String MSG_OP_STUDENT_ASSESSMENT_PERF = "student.assessment.performance";
    public static final String MSG_OP_SESSION_STATUS = "session.status";
    public static final String MSG_OP_USER_ALL_ASSESSMENT_SESSIONS = "user.all.assessment.sessions";
    public static final String MSG_OP_USER_ALL_COLLECTION_SESSIONS = "user.all.collection.sessions";
    public static final String MSG_OP_ALL_STUDENT_COURSE_PERF = "all.student.course.performance";
    public static final String MSG_OP_ALL_STUDENT_UNIT_PERF = "all.student.unit.performance";
    public static final String MSG_OP_ALL_STUDENT_LESSON_PERF = "all.student.lesson.performance";
    public static final String MSG_OP_ALL_STUDENT_COLLECTION_PERF = "all.student.collection.performance";
    public static final String MSG_OP_ALL_STUDENT_ASSESSMENT_PERF = "all.student.assessment.performance";
    public static final String MSG_OP_SESSION_TAXONOMY_REPORT = "session.taxonomy.report";
    public static final String MSG_OP_ALL_STUDENT_CLASSES_PERF = "all.class.student.performance";
    public static final String MSG_OP_STUDENT_LOC_ALL_CLASSES = "student.all.classes.location";
    public static final String MSG_OP_STUDENT_PERF_MULT_COLLECTION = "student.multiple.collection.performance";
    public static final String MSG_OP_STUDENT_PERF_COURSE_ASSESSMENT = "student.course.assessment.performance";
    public static final String MSG_OP_STUDENT_PERF_COURSE_COLLECTION = "student.course.collection.performance";
    
    public static final String MSG_OP_INDEPENDENT_LEARNER_COURSE_PERF = "independent.learner.course.performance";
    public static final String MSG_OP_INDEPENDENT_LEARNER_UNIT_PERF = "independent.learner.unit.performance";
    public static final String MSG_OP_INDEPENDENT_LEARNER_LESSON_PERF = "independent.learner.lesson.performance";
    public static final String MSG_OP_INDEPENDENT_LEARNER_ASSESSMENT_PERF = "independent.learner.assessment.performance";
    public static final String MSG_OP_INDEPENDENT_LEARNER_INDEPENDENT_ASSESSMENT_PERF = "independent.learner.independent.assessment.performance";
    public static final String MSG_OP_INDEPENDENT_LEARNER_COURSES = "independent.learner.courses";
    public static final String MSG_OP_IND_LEARNER_ALL_LOCATION = "independent.learner.all.location";
    public static final String MSG_OP_IND_LEARNER_ALL_PERFORMANCE = "independent.learner.all.performance";
    public static final String MSG_OP_IND_LEARNER_LOCATION_ALL_COURSES = "independent.learner.all.courses.location";
    public static final String MSG_OP_IND_LEARNER_LOCATION_ALL_IND_ASSESSMENTS = "independent.learner.all.ind.assessments.location";
    public static final String MSG_OP_IND_LEARNER_LOCATION_ALL_IND_COLLECTIONS = "independent.learner.all.ind.collections.location";    
    public static final String MSG_OP_IND_LEARNER_PERF_ALL_COURSES = "il.all.courses.perf";
    public static final String MSG_OP_IND_LEARNER_PERF_ALL_IND_ASSESSMENTS = "independent.learner.all.ind.assessments.performance";
    public static final String MSG_OP_IND_LEARNER_PERF_ALL_IND_COLLECTIONS = "independent.learner.all.ind.collections.performance";
    public static final String MSG_OP_IND_LEARNER_COURSE_ALL_COLLECTIONS_PERF = "independent.learner.course.all.collections.performance";
    public static final String MSG_OP_IND_LEARNER_COURSE_ALL_ASSESSMENTS_PERF = "independent.learner.course.all.assessments.performance";
    public static final String MSG_OP_IND_LEARNER_TAX_SUBJECTS = "independent.learner.taxonomy.subjects";
    public static final String MSG_OP_LEARNER_COURSES = "learner.courses";
    public static final String MSG_OP_IND_LEARNER_CURRENT_LOC = "independent.learner.current.loc";
    public static final String MSG_OP_IND_LEARNER_ALL_COURSES_PERF = "independent.learner.all.courses.performance";

    public static final String MSG_OP_IND_LEARNER_ASSESSMENT_SUMMARY = "independent.learner.assessment.summary";
    public static final String MSG_OP_IND_LEARNER_COLLECTION_SUMMARY = "independent.learner.collection.summary";
    public static final String MSG_OP_IND_LEARNER_ALL_ASSESSMENT_SESSIONS = "independent.learner.all.assessment.sessions";
    public static final String MSG_OP_IND_LEARNER_ALL_COLLECTION_SESSIONS = "independent.learner.all.collection.sessions";

    //Rubric Grading
    public static final String MSG_OP_RUBRICS_QUESTIONS_TO_GRADE = "rubric.questions.to.grade";
    public static final String MSG_OP_RUBRIC_QUESTIONS_STUDENTS_LIST = "rubric.questions.students.list";
    public static final String MSG_OP_RUBRIC_QUESTIONS_STUDENT_ANSWERS = "rubric.questions.student.answers";
    public static final String MSG_OP_RUBRIC_QUESTIONS_GRADE_SUMMARY = "rubric.questions.grade.summary";
    
    //DCA
    public static final String MSG_OP_DCA_STUDENT_COLLECTION_SUMMARY = "dca.student.collection.summary";
    public static final String MSG_OP_DCA_STUDENT_ASSESSMENT_SUMMARY = "dca.student.assessment.summary";
    public static final String MSG_OP_STUDENT_PERF_DAILY_CLASS_ACTIVITY = "student.daily.class.activity.performance";
    public static final String MSG_OP_DCA_SESSION_TAXONOMY_REPORT = "dca.session.taxonomy.report";
    
    //DATA REPORTS
    public static final String MSG_OP_NU_DATA_REPORT = "nu.data.report";
    public static final String MSG_OP_NU_COURSE_COMPETENCY_COMPLETION = "nu.course.competency.completion";
    public static final String MSG_OP_NU_COURSES_COMPETENCY_COMPLETION = "nu.courses.competency.completion";

    public static final String PORT = "port";
    public static final String HOST = "host";
    public static final String REDIS = "redis.config";
    public static final String REDIS_MAX_SIZE = "max.size";
    public static final String REDIS_MAX_IDLE = "max.idle";
    public static final String REDIS_MIN_IDLE = "min.idle";
    public static final String REDIS_MAX_WAIT_MILLIS = "max.wait.millis";

    private MessageConstants() {
        throw new AssertionError();
    }
}
