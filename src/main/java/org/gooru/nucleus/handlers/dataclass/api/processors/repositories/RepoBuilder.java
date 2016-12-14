package org.gooru.nucleus.handlers.dataclass.api.processors.repositories;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.AJRepoBuilder;

public class RepoBuilder {

    public StudentRepo buildStudentRepo(ProcessorContext context) {
        return AJRepoBuilder.buildStudentRepo(context);
    }
    
    public TeacherRepo buildTeacherRepo(ProcessorContext context) {
        return AJRepoBuilder.buildTeacherRepo(context);
    }

}
