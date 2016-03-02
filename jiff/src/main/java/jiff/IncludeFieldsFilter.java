package jiff;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;


public class IncludeFieldsFilter extends AbstractFieldListFilter {

    public IncludeFieldsFilter(Collection<String> fields) {
        super(fields);
    }

    public IncludeFieldsFilter(String...fields) {
        super(fields);
    }

    @Override
    public boolean includeField(List<String> fieldName) {
        for(List<String> pattern:fields)
            if(matches(pattern,fieldName))
                return true;
        return false;
    }
}
