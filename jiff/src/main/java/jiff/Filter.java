package jiff;

import java.util.List;

/**
 * Filter that determines whether to include field in comparison
 */
public interface Filter {
    boolean includeField(List<String> field);
}
