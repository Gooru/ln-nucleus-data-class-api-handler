
package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc;

import org.gooru.nucleus.handlers.dataclass.api.processors.ProcessorContext;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.InternalRepo;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.dbhandlers.DBHandlerBuilder;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.transactions.TransactionExecutor;
import org.gooru.nucleus.handlers.dataclass.api.processors.responses.MessageResponse;

/**
 * @author szgooru Created On 12-Apr-2019
 */
public class AJInternalRepo implements InternalRepo {

  private final ProcessorContext context;

  public AJInternalRepo(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public MessageResponse getAllClassPerformance() {
    return TransactionExecutor
        .executeTransaction(DBHandlerBuilder.buildInternalAllClassPerformanceHandler(context));
  }

  @Override
  public MessageResponse getClassDCAPerformance() {
    return TransactionExecutor
        .executeTransaction(DBHandlerBuilder.buildInternalAllClassDCAPerformanceHandler(context));

  }

}
