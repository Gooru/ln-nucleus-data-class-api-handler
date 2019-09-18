package org.gooru.nucleus.handlers.dataclass.api.processors.suggestions;

import java.util.List;
import org.gooru.nucleus.handlers.dataclass.api.processors.repositories.activejdbc.entities.AJEntityCollectionPerformance;
import io.vertx.core.json.JsonArray;

/**
 * @author renuka
 */
interface SuggestionsPerformanceResponseBuilder {

  JsonArray build();

  static SuggestionsPerformanceResponseBuilderImpl buildSuggestionPerformanceResponse( List<AJEntityCollectionPerformance> command) {
    return new SuggestionsPerformanceResponseBuilderImpl(command);
  }

}
