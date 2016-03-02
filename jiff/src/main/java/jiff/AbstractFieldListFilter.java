package jiff;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;


public abstract class AbstractFieldListFilter extends AbstractFieldFilter {

    protected List<List<String>> fields;
    
    public AbstractFieldListFilter(Collection<String> fields) {
        this.fields=new ArrayList<>(fields.size());
        for(String x:fields)
            this.fields.add(parse(x));
    }

    public AbstractFieldListFilter(String...fields) {
        this.fields=new ArrayList<>(fields.length);
        for(String x:fields)
            this.fields.add(parse(x));
    }

}
