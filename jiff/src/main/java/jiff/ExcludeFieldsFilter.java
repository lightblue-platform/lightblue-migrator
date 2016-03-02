package jiff;

import java.util.List;
import java.util.Collection;

public class ExcludeFieldsFilter extends AbstractFieldListFilter {

    public ExcludeFieldsFilter(Collection<String> fields) {
        super(fields);
    }

    public ExcludeFieldsFilter(String...fields) {
        super(fields);
    }

    @Override
    public boolean includeField(List<String> fieldName) {
        for(List<String> pattern:fields)
            if(matches(pattern,fieldName))
                return false;
        return true;
    }
}
