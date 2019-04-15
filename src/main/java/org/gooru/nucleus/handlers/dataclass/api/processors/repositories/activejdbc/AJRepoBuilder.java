package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.ReportRepo;

public final class AJRepoBuilder {

  private AJRepoBuilder() {
    throw new AssertionError();
  }

  public static ReportRepo buildReportRepo(ProcessorContext context) {
    return new AJReportRepo(context);
  }
}
