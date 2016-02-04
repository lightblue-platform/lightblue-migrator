package jiff;

/**
 * Filter that determines whether to include field in comparison
 */
public interface Filter {
    boolean includeField(String field);
}
