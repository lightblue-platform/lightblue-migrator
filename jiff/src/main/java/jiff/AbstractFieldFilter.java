package jiff;

public abstract class AbstractFieldFilter implements Filter {

    public static boolean matches(String pattern,String field) {
        int p=pattern.length();
        int f=field.length();
        if(p<=f) {
            int state=0; // 0: begin, 1: index, 2: field
            int ixp=0;
            int ixf=0;
            for(;ixp<p&&ixf<f;) {
                char pc=pattern.charAt(ixp);
                char fc=field.charAt(ixf);
                switch(state) {
                case 0: // begin
                    if(pc=='*') {
                        state=1;
                    } else {
                        state=2;
                    }
                    break;

                case 1: // index
                    if(Character.isDigit(fc)) {
                        // ok
                        ixf++;
                    } else if(fc=='.') {
                        ixf++;
                        ixp++;
                        if(ixp<p&&pattern.charAt(ixp)=='.') {
                        	ixp++;
                            state=0;
                        } else
                            return false;
                    } else
                        return false;
                    break;

                case 2: // field
                    if(fc==pc) {
                        ixf++;
                        ixp++;
                        if(fc=='.')
                            state=0;
                    } else {
                        return false;
                    }
                    break;
                }
            }
            if(ixp<p&&state==1)
            	ixp++;
            return ixp==p&&ixf==f;
        } else
            return false;
    }
}
