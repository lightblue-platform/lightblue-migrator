package com.redhat.lightblue.migrator.facade.methodcallstringifier;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory.Secret;

/**
 * Use ${link {@link LazyMethodCallStringifier} instead. This @{link
 * {@link MethodCallStringifier} does not support the {@link Secret}
 * annotations.
 *
 * @author mpatercz
 *
 */
@Deprecated
public class EagerMethodCallStringifier implements MethodCallStringifier {

    private static final Logger log = LoggerFactory.getLogger(EagerMethodCallStringifier.class);

    private final String methodCallString;

    public EagerMethodCallStringifier(String methodName, Object[] values) {
        this.methodCallString = methodCallToString(methodName, values);
    }

    public EagerMethodCallStringifier(String methodCallString) {
        this.methodCallString = methodCallString;
    }

    private String methodCallToString(String methodName, Object[] values) {
        try {
            StringBuilder str = new StringBuilder();
            str.append(methodName).append("(");
            Iterator<Object> it = Arrays.asList(values).iterator();
            while (it.hasNext()) {
                Object value = it.next();
                if (value != null && value.getClass().isArray()) {
                    if (value.getClass().getComponentType().isPrimitive()) {
                        // this is an array of primitives, convert to a meaningful string using reflection
                        String primitiveArrayType = value.getClass().getComponentType().getName();

                        StringBuilder pStr = new StringBuilder();
                        for (int i = 0; i < Array.getLength(value); i++) {
                            pStr.append(Array.get(value, i));
                            if (i != Array.getLength(value) - 1) {
                                pStr.append(", ");
                            }
                        }
                        str.append(primitiveArrayType).append("[").append(pStr.toString()).append("]");
                    } else {
                        str.append(Arrays.deepToString((Object[]) value));
                    }
                } else {
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

    /* (non-Javadoc)
     * @see com.redhat.lightblue.migrator.facade.MethodCallStringifier#toString()
     */
    @Override
    public String toString() {
        return methodCallString;
    }

    public static String stringifyMethodCall(final String methodName, final Object[] values) {
        return new EagerMethodCallStringifier(methodName, values).toString();
    }

}
