package com.redhat.lightblue.migrator.facade;

import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Provides means to access timeout configuration defined in a {@link Properties} object.</p>
 *
 * <p>Define bean level timeout example:</p>
 * <pre>
 *  com.redhat.lightblue.migrator.facade.timeout.CountryDAO=2000
 * </pre>
 *
 * <p>Define method level timeout example (takes precedence over bean level timeout):</p>
 * <pre>
 *  com.redhat.lightblue.migrator.facade.timeout.CountryDAO.getCountries=5000
 * </pre>
 *
 * <p>Zero or less means no timeout.</p>
 *
 * @author mpatercz
 *
 */
public class TimeoutConfiguration {

    private static final Logger log = LoggerFactory.getLogger(TimeoutConfiguration.class);

    public static final String TIMEOUT_CONFIG_PREFIX = "com.redhat.lightblue.migrator.facade.timeout.";
    private final String timeoutConfigBeanPrefix;

    private long defaultTimeoutMS;
    private String beanName;
    private Properties properties;

    private HashMap<String, Long> methodTimeouts = new HashMap<>();

    /**
     *
     * @param defaultTimeoutMS Use this timeout if nothing matches in the properties
     * @param beanName bean name to use, e.g. CountryDAO
     * @param properties properties read from a file with timeout settings. Can be null.
     */
    public TimeoutConfiguration(long defaultTimeoutMS, String beanName, Properties properties) {
        this.defaultTimeoutMS = defaultTimeoutMS;
        this.beanName = beanName;
        this.timeoutConfigBeanPrefix = TIMEOUT_CONFIG_PREFIX+beanName;
        if (properties != null)
            this.properties = properties;
        else
            this.properties = new Properties();

        log.info("Initialized TimeoutConfiguration for {}", timeoutConfigBeanPrefix);
    }

    public long getTimeoutMS(String methodName) {
        if (methodTimeouts.containsKey(methodName)) {
            return methodTimeouts.get(methodName);
        }

        String timeoutPropValue = properties.getProperty(timeoutConfigBeanPrefix+"."+methodName);

        if (timeoutPropValue == null) {
            if (log.isDebugEnabled())
                log.debug("Timeout config not found for method {}, trying default for this bean", methodName);

            timeoutPropValue = properties.getProperty(timeoutConfigBeanPrefix);
        }

        Long timeout;
        if (timeoutPropValue == null) {
            if (log.isDebugEnabled())
                log.debug("Timeout config not found for bean {} using global timeout", beanName);

            timeout = defaultTimeoutMS;
        } else {
            timeout = Long.parseLong(timeoutPropValue);
        }

        if (log.isDebugEnabled()) {
            log.debug("Setting timeout for {}.{} to {}ms", beanName, methodName, timeout);
        }

        methodTimeouts.put(methodName, timeout);

        return timeout;
    }

}
