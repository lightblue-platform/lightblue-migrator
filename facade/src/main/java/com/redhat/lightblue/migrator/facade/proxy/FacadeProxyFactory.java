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

public class FacadeProxyFactory {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface ReadOperation {}

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface WriteSingleOperation {
        String entityIdExtractorClassName();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface UpdateOperation {}

    private static class FacadeInvocationHandler<D> implements InvocationHandler {

        private static final Logger log = LoggerFactory.getLogger(FacadeInvocationHandler.class);

        private DAOFacadeBase<D> daoFacadeBase;

        public FacadeInvocationHandler(D legacyDAO, D lightblueDAO) {
            this.daoFacadeBase = new DAOFacadeBase<D>(legacyDAO, lightblueDAO);
        }

        public FacadeInvocationHandler(DAOFacadeBase<D> daoFacadeBase) {
            this.daoFacadeBase = daoFacadeBase;
        }

        // <className, entityIdExtractor>
        private HashMap<String, EntityIdExtractor> entityIdExtractors = new HashMap<>();

        private EntityIdExtractor lazyLoadEntityIdExtractor(String className) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
            if (entityIdExtractors.containsKey(className)) {
                return entityIdExtractors.get(className);
            }

            EntityIdExtractor extractor = (EntityIdExtractor) Class.forName(className).newInstance();
            entityIdExtractors.put(className, extractor);

            return extractor;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getDeclaredAnnotation(ReadOperation.class) != null) {
                return daoFacadeBase.callDAOReadMethod(method.getReturnType(), method.getName(), method.getParameterTypes(), args);
            }

            if (method.getDeclaredAnnotation(WriteSingleOperation.class) != null) {
                WriteSingleOperation a = method.getDeclaredAnnotation(WriteSingleOperation.class);
                // initialize entity extractor
                EntityIdExtractor e = lazyLoadEntityIdExtractor(a.entityIdExtractorClassName());
                return daoFacadeBase.callDAOCreateSingleMethod(e, method.getReturnType(), method.getName(), method.getParameterTypes(), args);
            }

            if (method.getDeclaredAnnotation(UpdateOperation.class) != null) {
                return daoFacadeBase.callDAOUpdateMethod(method.getReturnType(), method.getName(), method.getParameterTypes(), args);
            }

            log.debug("Not a facade operation, proxy passing to legacy");
            return method.invoke(daoFacadeBase.getLegacyDAO(), args);
        }

    }

    public static <D> D createFacadeProxy(DAOFacadeBase<D> daoFacadeBase, Class<D> daoClass) throws InstantiationException, IllegalAccessException {
        return (D) Proxy.newProxyInstance(daoClass.getClassLoader(), new Class[] {daoClass}, new FacadeInvocationHandler<D>(daoFacadeBase));
    }

    public static <D> D createFacadeProxy(D legacyDAO, D lightblueDAO, Class<D> daoClass) throws InstantiationException, IllegalAccessException {
        return createFacadeProxy(new DAOFacadeBase<D>(legacyDAO, lightblueDAO), daoClass);
    }

}
