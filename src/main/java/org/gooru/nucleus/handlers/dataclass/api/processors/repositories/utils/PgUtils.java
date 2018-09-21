package org.gooru.nucleus.handlers.dataclass.api.processors.repositories.utils;

import java.util.Iterator;
import java.util.List;

/**
 * Created by renuka on 18/9/18.
 */
public final class PgUtils {

    public static String listToPostgresArrayString(List<String> input) {
        int approxSize = ((input.size() + 1) * 36); // Length of UUID is around  36  chars
        Iterator<String> it = input.iterator();
        if (!it.hasNext()) {
            return "{}";
        }

        StringBuilder sb = new StringBuilder(approxSize);
        sb.append('{');
        for (;;) {
            String s = it.next();
            sb.append('"').append(s).append('"');
            if (!it.hasNext()) {
                return sb.append('}').toString();
            }
            sb.append(',');
        }
    }
    
    private PgUtils() {
        throw new AssertionError();
    }
}
