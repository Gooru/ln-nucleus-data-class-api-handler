package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.converters;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hazelcast.util.CollectionUtil;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class ValueMapper {

  public static JsonObject map(Map<?, ?> attributesMap, Map<?, ?> databaseResult) {
    JsonObject result = new JsonObject();
    for (Entry<?, ?> resultRow : attributesMap.entrySet()) {
      result.put(resultRow.getValue().toString(), castValue(databaseResult.get(resultRow.getKey())));
    }
    return result;
  }

  @SuppressWarnings("rawtypes") 
  public static JsonArray map(Map<?, ?> attributesMap, List<Map> databaseResult) {
    JsonArray arrayResult = new JsonArray();
    if (CollectionUtil.isNotEmpty(databaseResult)) {
      databaseResult.forEach(databaseResultRow -> {
        JsonObject result = new JsonObject();
        for (Entry<?, ?> resultRow : attributesMap.entrySet()) {
          result.put(resultRow.getValue().toString(), castValue(databaseResultRow.get(resultRow.getKey())));
        }
        arrayResult.add(result);
      });
    }
    return arrayResult;
  }

  private static Object castValue(Object val) {
    /**
     * Other data types to be checked..
     */
    if (val instanceof BigDecimal) {
      return ((Number) val).longValue();
    }
    return val;
  }
}
