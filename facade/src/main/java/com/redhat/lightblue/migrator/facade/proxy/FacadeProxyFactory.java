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
import com.redhat.lightblue.migrator.features.LightblueMigration;

/**
 * Creates a dynamic proxy implementing given interface. The calls to the
 * interface apis will be directed to the underlying {@link ServiceFacade}
 * according to the type of the operation specified using annotations.
 *
 * @author mpatercz
 *
 */
public class FacadeProxyFactory {

    /**
     * Indicates this api performs a read operation. See null null     {@link ServiceFacade#callSvcMethod(FacadeOperation, boolean, Class, String, Class[], Object...) for details.
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
     * Indicates this api performs a write operation. See null null     {@link ServiceFacade#callSvcMethod(FacadeOperation, boolean, Class, String, Class[], Object...) for details.
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
     * Pass the call directly to destination or source service. Ignores
     * migration phases (togglz). Note that @Direct(target=SOURCE) is the same
     * as no annotation.
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
                        log.debug("Method \"{}\" is explicitly annotated to call legacy", method);
                        Method legacyMethod = svcFacade.getLegacySvc().getClass().getMethod(method.getName(), method.getParameterTypes());
                        return legacyMethod.invoke(svcFacade.getLegacySvc(), args);
                    }
                    case LIGHTBLUE: {
                        log.debug("Method \"{}\" is explicitly annotated to call lightblue", method);
                        Method destinationMethod = svcFacade.getLightblueSvc().getClass().getMethod(method.getName(), method.getParameterTypes());
                        return destinationMethod.invoke(svcFacade.getLightblueSvc(), args);
                    }
                }
            }

            if (method.isAnnotationPresent(ReadOperation.class)) {
                ReadOperation ro = method.getAnnotation(ReadOperation.class);
                log.debug("Performing parallel=" + ro.parallel() + " " + FacadeOperation.READ + " operation");
                return svcFacade.callSvcMethod(FacadeOperation.READ, ro.parallel(), method, args);
            }

            if (method.isAnnotationPresent(WriteOperation.class)) {
                WriteOperation wo = method.getAnnotation(WriteOperation.class);
                log.debug("Performing parallel=" + wo.parallel() + " " + FacadeOperation.WRITE + " operation");
                return svcFacade.callSvcMethod(FacadeOperation.WRITE, wo.parallel(), method, args);
            }

            if (!LightblueMigration.shouldReadSourceEntity() && !LightblueMigration.shouldWriteSourceEntity()) {
                // this method is not annotated and legacy/source is no more
                throw new IllegalStateException("Method \""+method+"\" is not annotated for facade and legacy source is disabled!");
            } else {
                // this can cause problems when entering proxy phase
                log.warn("Method \"{}\" is not annotated for facade. Proxy passing to legacy.", method);

                Method legacyMethod = svcFacade.getLegacySvc().getClass().getMethod(method.getName(), method.getParameterTypes());
                return legacyMethod.invoke(svcFacade.getLegacySvc(), args);
            }
        }

    }

    /**
     * Create facade proxy from {@link ServiceFacade}.
     *
     * Java does not allow typed argument with both typed and static bounds,
     * i.e. <D extends SharedStoreSetter & T> does not compile. To work around
     * this limitation and to ensure that facaded services implement both the
     * facade interface and {@link SharedStoreSetter}, I'm using
     * <T extends D,D extends SharedStoreSetter>. This makes the facade
     * interface - T - require to implement {@link SharedStoreSetter}. It does
     * not logically belong to the facade interface, but I guess I can live with
     * that.
     *
     * @param svcFacade initialized with services implementing the
     * facadeInterface
     * @param facadeInterface has to implement {@link SharedStoreSetter}
     * @return facaded service proxy
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    @SuppressWarnings("unchecked")
    public static <T extends D, D extends SharedStoreSetter> D createFacadeProxy(ServiceFacade<D> svcFacade, Class<T> facadeInterface) throws InstantiationException, IllegalAccessException {
        return (D) Proxy.newProxyInstance(facadeInterface.getClassLoader(), new Class[]{facadeInterface}, new FacadeInvocationHandler<D>(svcFacade));
    }

    /**
     * Create a facade proxy.
     *
     * Java does not allow typed argument with both typed and static bounds,
     * i.e. <D extends SharedStoreSetter & T> does not compile. To work around
     * this limitation and to ensure that facaded services implement both the
     * facade interface and {@link SharedStoreSetter}, I'm using
     * <T extends D,D extends SharedStoreSetter>. This makes the facade
     * interface - T - require to implement {@link SharedStoreSetter}. It does
     * not logically belong to the facade interface, but I guess I can live with
     * that.
     *
     * @param legacySvc has to implement facadeInterface
     * @param lightblueSvc has to implement facadeInterface
     * @param facadeInterface has to implement {@link SharedStoreSetter}
     * @param properties
     * @return facaded service proxy
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static <T extends D, D extends SharedStoreSetter> D createFacadeProxy(D legacySvc, D lightblueSvc, Class<T> facadeInterface, Properties properties) throws InstantiationException, IllegalAccessException {
        return createFacadeProxy(new ServiceFacade<D>(legacySvc, lightblueSvc, facadeInterface.getSimpleName(), properties), facadeInterface);
    }

}
