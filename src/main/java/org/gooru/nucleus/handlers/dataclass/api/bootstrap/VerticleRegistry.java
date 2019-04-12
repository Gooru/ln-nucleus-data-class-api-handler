package org.gooru.nucleus.handlers.dataclass.api.bootstrap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This is repository for verticles which needs to be deployed by {@link DeployVerticle}
 *
 * @author Insights Team
 */
public class VerticleRegistry implements Iterable<String> {

  private static final String AUTH_VERTICLE =
      "org.gooru.nucleus.handlers.dataclass.api.bootstrap.AuthVerticle";

  private static final String CLASS_DATA_REPORT_VERTICLE =
      "org.gooru.nucleus.handlers.dataclass.api.bootstrap.DataClassReadApiVerticle";

  private final Iterator<String> internalIterator;

  public VerticleRegistry() {
    List<String> initializers = new ArrayList<>();
    initializers.add(AUTH_VERTICLE);
    initializers.add(CLASS_DATA_REPORT_VERTICLE);
    internalIterator = initializers.iterator();
  }

  @Override
  public Iterator<String> iterator() {
    return new Iterator<String>() {

      @Override
      public boolean hasNext() {
        return internalIterator.hasNext();
      }

      @Override
      public String next() {
        return internalIterator.next();
      }

    };
  }

}
