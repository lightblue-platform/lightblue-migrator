package com.redhat.lightblue.migrator.facade.methodcallstringifier;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory.Secret;

/* (non-Javadoc)
 * @see com.redhat.lightblue.migrator.facade.MethodCallStringifier
 */
public class LazyMethodCallStringifier implements MethodCallStringifier {

    private static final Logger log = LoggerFactory.getLogger(LazyMethodCallStringifier.class);

    private final Method method;
    private final Object[] values;
    private final String stringifiedMethodCall;

    public LazyMethodCallStringifier(Method method, Object[] values) {
        this.method = method;
        this.values = values;
        this.stringifiedMethodCall = null;
    }

    public LazyMethodCallStringifier(String stringifiedMethodCall) {
        this.method = null;
        this.values = null;
        this.stringifiedMethodCall = stringifiedMethodCall;
    }

    public LazyMethodCallStringifier() {
        this("");
    }

    /* (non-Javadoc)
     * @see com.redhat.lightblue.migrator.facade.MethodCallStringifier#toString()
     */
    @Override
    public String toString() {

        if (stringifiedMethodCall != null)
            return stringifiedMethodCall;

        try {
            StringBuilder str = new StringBuilder();
            str.append(method.getName()).append("(");
            Iterator<Object> it = Arrays.asList(values).iterator();
            Iterator<Annotation[]> annotations = Arrays.asList(method.getParameterAnnotations()).iterator();
            while(it.hasNext()) {
                Object value = it.next();
                boolean isSecret = false;
                for (Annotation a: annotations.next()) {
                    if (a instanceof Secret) {
                        isSecret=true;
                        break;
                    }
                }

                if (isSecret) {
                    str.append("****");
                } else {
                    if (value != null && value.getClass().isArray())
                        if (value.getClass().getComponentType().isPrimitive()) {
                            // this is an array of primitives, convert to a meaningful string using reflection
                            String primitiveArrayType = value.getClass().getComponentType().getName();

                            StringBuilder pStr = new StringBuilder();
                            for (int i = 0; i < Array.getLength(value); i ++) {
                                pStr.append(Array.get(value, i));
                                if (i != Array.getLength(value)-1) {
                                    pStr.append(", ");
                                }
                            }
                            str.append(primitiveArrayType).append("[").append(pStr.toString()).append("]");
                        }
                        else {
                            str.append(Arrays.deepToString((Object[])value));
                        }
                    else
                        str.append(value);
                }

                if (it.hasNext()) {
                    str.append(", ");
                }
            }
            str.append(")");
            return str.toString();
        } catch (Exception e) {
            log.error("Creating method call string failed", e);
            return "<creating method call string failed>";
        }
    }

    public static String stringifyMethodCall(final Method method, final Object[] values) {
        return new LazyMethodCallStringifier(method, values).toString();
    }

}
