package com.redhat.lightblue.migrator.facade.proxy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.migrator.facade.ServiceFacade;
import com.redhat.lightblue.migrator.facade.ServiceFacade.FacadeOperation;
import com.redhat.lightblue.migrator.facade.sharedstore.SharedStoreSetter;

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

    /**
     * Pass the call directly to destination or source service. Ignores migration phases (togglz).
     * Note that @Direct(target=SOURCE) is the same as no annotation.
     *
     * @author mpatercz
     *
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface DirectOperation {
        enum Target {
            LEGACY, LIGHTBLUE;
        }
        Target target();
    }

    /**
     * Secret parameters are logged as ****.
     *
     * @author mpatercz
     *
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.PARAMETER)
    public static @interface Secret {
    }

    private static class FacadeInvocationHandler<D extends SharedStoreSetter> implements InvocationHandler {

        private static final Logger log = LoggerFactory.getLogger(FacadeInvocationHandler.class);

        private ServiceFacade<D> svcFacade;

        public FacadeInvocationHandler(ServiceFacade<D> svcFacade) {
            this.svcFacade = svcFacade;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.isAnnotationPresent(DirectOperation.class)) {
                DirectOperation.Target t = method.getAnnotation(DirectOperation.class).target();

                switch (t) {
                    case LEGACY: {
                        log.debug("Not a facade operation, proxy passing to legacy");
                        Method legacyMethod = svcFacade.getLegacySvc().getClass().getMethod(method.getName(), method.getParameterTypes());
                        return legacyMethod.invoke(svcFacade.getLegacySvc(), args);
                    }
                    case LIGHTBLUE: {
                        log.debug("Not a facade operation, proxy passing to lightblue");
                        Method destinationMethod = svcFacade.getLightblueSvc().getClass().getMethod(method.getName(), method.getParameterTypes());
                        return destinationMethod.invoke(svcFacade.getLightblueSvc(), args);
                    }
                }
            }

            if (method.isAnnotationPresent(ReadOperation.class)) {
                ReadOperation ro = method.getAnnotation(ReadOperation.class);
                log.debug("Performing parallel="+ro.parallel()+" "+FacadeOperation.READ+" operation");
                return svcFacade.callSvcMethod(FacadeOperation.READ, ro.parallel(), method, args);
            }

            if (method.isAnnotationPresent(WriteOperation.class)) {
                WriteOperation wo = method.getAnnotation(WriteOperation.class);
                log.debug("Performing parallel="+wo.parallel()+" "+FacadeOperation.WRITE+" operation");
                return svcFacade.callSvcMethod(FacadeOperation.WRITE, wo.parallel(), method, args);
            }

            log.debug("Not a facade operation, proxy passing to legacy");

            Method legacyMethod = svcFacade.getLegacySvc().getClass().getMethod(method.getName(), method.getParameterTypes());
            return legacyMethod.invoke(svcFacade.getLegacySvc(), args);
        }

    }

    @SuppressWarnings("unchecked")
    public static <T,D extends SharedStoreSetter> T createFacadeProxy(ServiceFacade<D> svcFacade, Class<T> facadeInterface) throws InstantiationException, IllegalAccessException {
        return (T) Proxy.newProxyInstance(facadeInterface.getClassLoader(), new Class[] {facadeInterface}, new FacadeInvocationHandler<D>(svcFacade));
    }

    public static <T,D extends SharedStoreSetter> T createFacadeProxy(D legacySvc, D lightblueSvc, Class<T> facadeInterface, Properties properties) throws InstantiationException, IllegalAccessException {
        return createFacadeProxy(new ServiceFacade<D>(legacySvc, lightblueSvc, facadeInterface.getCanonicalName(), properties), facadeInterface);
    }

}
