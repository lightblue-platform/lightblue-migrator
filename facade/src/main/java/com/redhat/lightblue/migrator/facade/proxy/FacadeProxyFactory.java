package com.redhat.lightblue.migrator.facade.proxy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.migrator.facade.DAOFacadeBase;
import com.redhat.lightblue.migrator.facade.EntityIdExtractor;

/**
 * Creates a dynamic proxy implementing given interface. The calls to the interface apis will be directed
 * to the underlying {@link DAOFacadeBase} according to the type of the operation specified using annotations.
 *
 * @author mpatercz
 *
 */
public class FacadeProxyFactory {

    /**
     * Indicates this api performs a read operation. See
     * {@link DAOFacadeBase#callDAOReadMethod(Class, String, Class[], Object...)} for details.
     *
     * @author mpatercz
     *
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface ReadOperation {}

    /**
     * Indicates this api performs a write operation for a single entity. See
     * {@link DAOFacadeBase#callDAOCreateSingleMethod(EntityIdExtractor, Class, String, Class[], Object...)} for details.
     *
     * @author mpatercz
     *
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface WriteSingleOperation {
        Class<? extends EntityIdExtractor<?>> entityIdExtractorClass();
    }

    /**
     * Indicates this api performs an update operation. See
     * {@link DAOFacadeBase#callDAOUpdateMethod(Class, String, Class[], Object...)} for details.
     *
     * @author mpatercz
     *
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface UpdateOperation {}

    private static class FacadeInvocationHandler<D> implements InvocationHandler {

        private static final Logger log = LoggerFactory.getLogger(FacadeInvocationHandler.class);

        private DAOFacadeBase<D> daoFacadeBase;

        public FacadeInvocationHandler(DAOFacadeBase<D> daoFacadeBase) {
            this.daoFacadeBase = daoFacadeBase;
        }

        private HashMap<Class<? extends EntityIdExtractor<?>>, EntityIdExtractor<?>> entityIdExtractors = new HashMap<>();

        private EntityIdExtractor<?> lazyLoadEntityIdExtractor(Class<? extends EntityIdExtractor<?>> clazz) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
            if (entityIdExtractors.containsKey(clazz)) {
                return entityIdExtractors.get(clazz);
            }

            EntityIdExtractor<?> extractor = (EntityIdExtractor<?>) clazz.newInstance();
            entityIdExtractors.put(clazz, extractor);

            return extractor;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.isAnnotationPresent(ReadOperation.class)) {
                return daoFacadeBase.callDAOReadMethod(method.getReturnType(), method.getName(), method.getParameterTypes(), args);
            }

            if (method.isAnnotationPresent(WriteSingleOperation.class)) {
                WriteSingleOperation a = method.getAnnotation(WriteSingleOperation.class);
                // initialize entity extractor
                @SuppressWarnings("rawtypes")
                EntityIdExtractor e = lazyLoadEntityIdExtractor(a.entityIdExtractorClass());
                @SuppressWarnings("unchecked")
                Object ret = daoFacadeBase.callDAOCreateSingleMethod(e, method.getReturnType(), method.getName(), method.getParameterTypes(), args);
                return ret;
            }

            if (method.isAnnotationPresent(UpdateOperation.class)) {
                return daoFacadeBase.callDAOUpdateMethod(method.getReturnType(), method.getName(), method.getParameterTypes(), args);
            }

            log.debug("Not a facade operation, proxy passing to legacy");
            return method.invoke(daoFacadeBase.getLegacyDAO(), args);
        }

    }

    @SuppressWarnings("unchecked")
    public static <D> D createFacadeProxy(DAOFacadeBase<D> daoFacadeBase, Class<D> daoClass) throws InstantiationException, IllegalAccessException {
        return (D) Proxy.newProxyInstance(daoClass.getClassLoader(), new Class[] {daoClass}, new FacadeInvocationHandler<D>(daoFacadeBase));
    }

    public static <D> D createFacadeProxy(D legacyDAO, D lightblueDAO, Class<D> daoClass) throws InstantiationException, IllegalAccessException {
        return createFacadeProxy(new DAOFacadeBase<D>(legacyDAO, lightblueDAO), daoClass);
    }

}
