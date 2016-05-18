package com.redhat.lightblue.migrator;

import java.util.List;

/**
 * Keeps the inconsistent field, its source and destination values
 */
public class Inconsistency {
    private final String field;
    private final Object sourceValue;
    private final Object destValue;

    public Inconsistency(String field, Object sourceValue, Object destValue) {
        this.field = field;
        this.sourceValue = sourceValue;
        this.destValue = destValue;
    }

    public String getField() {
        return field;
    }

    public Object getSourceValue() {
        return sourceValue;
    }

    public Object getDestValue() {
        return destValue;
    }

    @Override
    public String toString() {
        return "field=" + field + ", sourceValue=" + sourceValue + " destValue=" + destValue;
    }

    public static String getPathList(List<Inconsistency> l) {
        StringBuilder bld = new StringBuilder();
        boolean first = true;
        for (Inconsistency x : l) {
            if (first) {
                first = false;
            } else {
                bld.append(',');
            }
            bld.append(x.field);
        }
        return bld.toString();
    }

    public static String getMismatchedValues(List<Inconsistency> l) {
        StringBuilder bld = new StringBuilder();
        boolean first = true;
        for (Inconsistency x : l) {
            if (first) {
                first = false;
            } else {
                bld.append(',');
            }
            bld.append("s:").append(x.sourceValue == null ? "null" : x.sourceValue.toString()).
                    append(" d:").append(x.destValue == null ? "null" : x.destValue.toString());
        }
        return bld.toString();
    }
}
