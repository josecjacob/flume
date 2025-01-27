package org.apache.flume.conf;

import org.apache.flume.Context;

/**
 * Methods for working with {@link Configurable}s.
 */
public class Configurables {

  /**
   * Check that {@code target} implements {@link Configurable} and, if so, ask
   * it to configure itself using the supplied {@code context}.
   * 
   * @param target
   *          An object that potentially implements Configurable.
   * @param context
   *          The configuration context
   * @return true if {@code target} implements Configurable, false otherwise.
   */
  public static boolean configure(Object target, Context context) {
    if (target instanceof Configurable) {
      ((Configurable) target).configure(context);
      return true;
    }

    return false;
  }

  public static void ensureRequiredNonNull(Context context, String... keys) {
    for (String key : keys) {
      if (!context.getParameters().containsKey(key)
          || context.getParameters().get(key) == null) {

        throw new IllegalArgumentException("Required parameter " + key
            + " must exist and may not be null");
      }
    }
  }

  public static void ensureOptionalNonNull(Context context, String... keys) {
    for (String key : keys) {
      if (context.getParameters().containsKey(key)
          && context.getParameters().get(key) == null) {

        throw new IllegalArgumentException("Optional parameter " + key
            + " may not be null");
      }
    }
  }

}
