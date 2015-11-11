package jiff;

import java.util.Set;
import java.util.HashSet;


public class IncludeFieldsFilter extends AbstractFieldFilter {

    private Set<String> fields;
    
    public IncludeFieldsFilter(Set<String> fields) {
        this.fields=fields;
    }

    public IncludeFieldsFilter(String...fields) {
        this.fields=new HashSet<>();
        for(String x:fields)
            this.fields.add(x);
    }

    @Override
    public boolean includeField(String fieldName) {
        for(String pattern:fields)
            if(matches(pattern,fieldName))
                return true;
        return false;
    }
}
