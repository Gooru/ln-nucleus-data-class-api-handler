package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.TeacherRepo;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandlerBuilder;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.transactions.TransactionExecutor;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;

/**
 * Created by mukul@gooru
 */
 class AJTeacherRepo implements TeacherRepo {
	    private final ProcessorContext context;

	    public AJTeacherRepo(ProcessorContext context) {
	        this.context = context;
	    }

	    @Override
	    public MessageResponse getAllStudentPerfInCourse() {
	        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildAllStudentCoursePerfHandler(context));
	    }

	    @Override
	    public MessageResponse getAllStudentPerfInUnit() {
	        return TransactionExecutor.executeTransaction(DBHandlerBuilder.buildAllStudentUnitPerfHandler(context));
	    }

}
