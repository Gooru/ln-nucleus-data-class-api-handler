package org.gooru.nucleus.handlers.dataclass.api.processors.repositories;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.AJRepoBuilder;

public class RepoBuilder {
  public ReportRepo buildReportRepo(ProcessorContext context) {
    return AJRepoBuilder.buildReportRepo(context);
  }
  
  public InternalRepo buildInternalRepo(ProcessorContext context) {
    return AJRepoBuilder.buildInternalRepo(context);
  }
}
