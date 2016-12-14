package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.StudentRepo;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.TeacherRepo;

public final class AJRepoBuilder {

    private AJRepoBuilder() {
        throw new AssertionError();
    }

    public static StudentRepo buildStudentRepo(ProcessorContext context) {
        return new AJStudentRepo(context);
    }
    
    public static TeacherRepo buildTeacherRepo(ProcessorContext context) {
        return new AJTeacherRepo(context);
    }
}
