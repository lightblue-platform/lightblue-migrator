package jiff;

import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;

public abstract class AbstractFieldFilter implements Filter {
    public static List<String> parse(String field) {
        ArrayList<String> list=new ArrayList<>(32);
        StringTokenizer tok=new StringTokenizer(field,". ");
        while(tok.hasMoreTokens())
            list.add(tok.nextToken());
        return list;
    }

    public static boolean matches(List<String> pattern,List<String> field) {
        int n=pattern.size();
        if(n==field.size()) {
            for(int i=0;i<n;i++) {
                String p=pattern.get(i);
                String f=field.get(i);
                if(!(p.equals(f)||p.equals("*")))
                    return false;
            }
        }
        return true;
    }
}
