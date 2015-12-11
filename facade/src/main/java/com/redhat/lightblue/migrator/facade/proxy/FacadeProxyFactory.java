package com.redhat.lightblue.migrator.facade.proxy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.migrator.facade.ServiceFacade;
import com.redhat.lightblue.migrator.facade.ServiceFacade.FacadeOperation;

/**
 * Creates a dynamic proxy implementing given interface. The calls to the interface apis will be directed
 * to the underlying {@link ServiceFacade} according to the type of the operation specified using annotations.
 *
 * @author mpatercz
 *
 */
public class FacadeProxyFactory {

    /**
     * Indicates this api performs a read operation. See
     * {@link ServiceFacade#callSvcMethod(FacadeOperation, boolean, Class, String, Class[], Object...) for details.
     *
     * Set parallel to true if services do not need to share state.
     *
     * @author mpatercz
     *
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface ReadOperation {
        boolean parallel() default false;
    }

    /**
     * Indicates this api performs a write operation. See
     * {@link ServiceFacade#callSvcMethod(FacadeOperation, boolean, Class, String, Class[], Object...) for details.
     *
     * Set parallel to true if services do not need to share state.
     *
     * @author mpatercz
     *
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface WriteOperation {
        boolean parallel() default false;
    }

    private static class FacadeInvocationHandler<D> implements InvocationHandler {

        private static final Logger log = LoggerFactory.getLogger(FacadeInvocationHandler.class);

        private ServiceFacade<D> daoFacadeBase;

        public FacadeInvocationHandler(ServiceFacade<D> daoFacadeBase) {
            this.daoFacadeBase = daoFacadeBase;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.isAnnotationPresent(ReadOperation.class)) {
                ReadOperation ro = method.getAnnotation(ReadOperation.class);
                log.debug("Performing parallel="+ro.parallel()+" "+FacadeOperation.READ+" operation");
                return daoFacadeBase.callSvcMethod(FacadeOperation.READ, ro.parallel(), method.getReturnType(), method.getName(), method.getParameterTypes(), args);
            }

            if (method.isAnnotationPresent(WriteOperation.class)) {
                WriteOperation wo = method.getAnnotation(WriteOperation.class);
                log.debug("Performing parallel="+wo.parallel()+" "+FacadeOperation.WRITE+" operation");
                return daoFacadeBase.callSvcMethod(FacadeOperation.WRITE, wo.parallel(), method.getReturnType(), method.getName(), method.getParameterTypes(), args);
            }

            log.debug("Not a facade operation, proxy passing to legacy");

            Method legacyMethod = daoFacadeBase.getLegacySvc().getClass().getMethod(method.getName(), method.getParameterTypes());
            return legacyMethod.invoke(daoFacadeBase.getLegacySvc(), args);
        }

    }

    @SuppressWarnings("unchecked")
    public static <D> D createFacadeProxy(ServiceFacade<D> svcFacade, Class<? extends D> svcClass) throws InstantiationException, IllegalAccessException {
        return (D) Proxy.newProxyInstance(svcClass.getClassLoader(), new Class[] {svcClass}, new FacadeInvocationHandler<D>(svcFacade));
    }

    public static <D> D createFacadeProxy(D legacySvc, D lightblueSvc, Class<? extends D> svcClass) throws InstantiationException, IllegalAccessException {
        return createFacadeProxy(new ServiceFacade<D>(legacySvc, lightblueSvc, svcClass), svcClass);
    }

}
