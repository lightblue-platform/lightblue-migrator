package com.redhat.lightblue.migrator.facade;

import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.lightblue.migrator.facade.ServiceFacade.FacadeOperation;

/**
 * <p>
 * Provides means to access timeout configuration defined in a
 * {@link Properties} object.</p>
 *
 * <p>
 * Define bean level timeout example:</p>
 * <pre>
 *  com.redhat.lightblue.migrator.facade.timeout.CountryDAO=2000
 * </pre>
 *
 * <p>
 * Define method level timeout example (takes precedence over bean level
 * timeout):</p>
 * <pre>
 *  com.redhat.lightblue.migrator.facade.timeout.CountryDAO.getCountries=5000
 * </pre>
 *
 * <p>
 * Zero or less means no timeout.</p>
 *
 * @author mpatercz
 *
 */
public class TimeoutConfiguration {

    private static final Logger log = LoggerFactory.getLogger(TimeoutConfiguration.class);

    public enum Type {
        timeout, slowwarning;
    }

    public static final String CONFIG_PREFIX = "com.redhat.lightblue.migrator.facade.";

    private long defaultTimeoutMS;
    private String beanName;
    private Properties properties;
    private boolean interruptOnTimeout = true;

    private HashMap<String, Long> methodTimeouts = new HashMap<>();

    /**
     *
     * @param defaultTimeoutMS Use this timeout if nothing matches in the
     * properties
     * @param beanName bean name to use, e.g. CountryDAO
     * @param properties properties read from a file with timeout settings. Can
     * be null.
     */
    public TimeoutConfiguration(long defaultTimeoutMS, String beanName, Properties properties) {
        this.defaultTimeoutMS = defaultTimeoutMS;
        this.beanName = beanName;
        if (properties != null) {
            this.properties = properties;
        } else {
            this.properties = new Properties();
        }

        if (properties != null) {
            // interruptOnTimeout key does not include bean name - it is global
            interruptOnTimeout = Boolean.parseBoolean(properties.getProperty(TimeoutConfiguration.CONFIG_PREFIX+".timeout.interruptOnTimeout", "true"));
        }

        log.info("Initialized TimeoutConfiguration for {}, interruptOnTimeout={}", beanName, interruptOnTimeout);
    }

    /**
     * Return timeout or slowwarning value. First checks if timeout was
     * configured for that method explicitly. If not, looks for timeout defined
     * for operation type (read/write). If not, looks for a timeout defined for
     * entire bean. If that is not set, takes a default global timeout.
     *
     * @param methodName to lookup timeout configuration by name
     * @param op to lookup timeout configuration by operation
     * @param type to lookup timeout configuration by type
     * @return
     */
    public long getMS(String methodName, FacadeOperation op, Type type) {
        String cacheKey = type + "-" + methodName;

        if (methodTimeouts.containsKey(cacheKey)) {
            return methodTimeouts.get(cacheKey);
        }

        String configurationKeyPrefix = CONFIG_PREFIX+type+"."+beanName;

        String timeoutPropValue = properties.getProperty(configurationKeyPrefix + "." + methodName);

        if (timeoutPropValue == null && op != null) {
            if (log.isDebugEnabled()) {
                log.debug("{} config not found for method {}, trying default for {} operations for this bean", type, methodName, op);
            }

            timeoutPropValue = properties.getProperty(configurationKeyPrefix + "." + op);
        }

        if (timeoutPropValue == null) {
            if (log.isDebugEnabled()) {
                log.debug("{} config not found for method {}, trying default for this bean", type, methodName);
            }

            timeoutPropValue = properties.getProperty(configurationKeyPrefix);
        }

        Long timeout;
        if (timeoutPropValue == null) {
            if (log.isDebugEnabled()) {
                log.debug("{} config not found for bean {} using global timeout", type, beanName);
            }

            switch (type) {
                case timeout: {
                    timeout = defaultTimeoutMS; break;
                }
                case slowwarning: {
                    timeout = 2 * defaultTimeoutMS; break;
                }
                default:
                    throw new IllegalArgumentException("Type " + type + " not known!");
            }

        } else {
            timeout = Long.parseLong(timeoutPropValue);
        }

        if (log.isDebugEnabled()) {
            log.debug("Setting {} for {}.{} to {}ms", type, beanName, methodName, timeout);
        }

        methodTimeouts.put(cacheKey, timeout);

        return timeout;
    }

    /**
     * See ${link
     * {@link TimeoutConfiguration#getMS(String, FacadeOperation, Type)}
     *
     * @param methodName
     * @param op
     * @return
     */
    public long getTimeoutMS(String methodName, FacadeOperation op) {
        return getMS(methodName, op, Type.timeout);
    }

    /**
     * See ${link
     * {@link TimeoutConfiguration#getMS(String, FacadeOperation, Type)}
     *
     * @param methodName
     * @param op
     * @return
     */
    public long getSlowWarningMS(String methodName, FacadeOperation op) {
        return getMS(methodName, op, Type.slowwarning);
    }

    public boolean isInterruptOnTimeout() {
        return interruptOnTimeout;
    }

    public void setInterruptOnTimeout(boolean interruptOnTimeout) {
        this.interruptOnTimeout = interruptOnTimeout;
    }

}
