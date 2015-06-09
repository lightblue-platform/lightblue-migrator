package com.redhat.lightblue.migrator;

import java.util.List;
import java.util.StringTokenizer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;


public class Identity {

    private final Object[] values;

    public Identity(Object[] values) {
        this.values=values;
    }

    public Identity(JsonNode doc,List<String> identityFields) {
        values=new Object[identityFields.size()];
        int i=0;
        for(String field:identityFields) {
            JsonNode n=getFieldValue(doc,field);
            if(n==null||n instanceof NullNode)
                values[i]=null;
            else
                values[i]=n.asText();
            i++;
        }
    }

    public Object get(int i) {
        return values[i];
    }

    public int hashCode() {
        int v=37;
        for(Object x:values)
            if(x!=null)
                v*=x.hashCode();
        return v;
    }

    public boolean equals(Object x) {
        if(x instanceof Identity) {
            Identity id=(Identity)x;
            if(id.values.length==values.length) {
                for(int i=0;i<id.values.length;i++)
                    if( !((values[i]==null&&id.values[i]==null) ||
                          (values[i]!=null&&values[i].equals(id.values[i])) ) )
                        return false;
                return true;
            }
        }
        return false;
    }

    public String toString() {
        StringBuilder bld=new StringBuilder();
        for(int i=0;i<values.length;i++) {
            if(i>0)
                bld.append(',');
            bld.append(values[i]==null?"null":values[i].toString());
        }
        return bld.toString();
    }

    /**
     * Ooes not do array index lookup!
     */
    public static JsonNode getFieldValue(JsonNode doc,String field) {
        StringTokenizer tkz=new StringTokenizer(field,". ");
        JsonNode trc=doc;
        while(tkz.hasMoreTokens()&&trc!=null) {
            String tok=tkz.nextToken();
            trc=trc.get(tok);
        }
        return trc;
    }

}
