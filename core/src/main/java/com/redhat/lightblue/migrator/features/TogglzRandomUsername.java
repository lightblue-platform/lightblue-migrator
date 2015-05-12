package com.redhat.lightblue.migrator.features;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates random username for togglz gradual activation strategy and stores it in ThreadLocal. Threads are
 * pooled and reused, so you need to call init method each time a decision is to be made where to send the
 * request (source or destination/lightblue). Beginning of a public DAO method is a good place.
 *
 * @author mpatercz
 *
 */
public abstract class TogglzRandomUsername {

    private static ThreadLocal<String> threadLocalUsername = new ThreadLocal<String>();

    private static Random random = new Random();

    public static final Logger log = LoggerFactory.getLogger(TogglzRandomUsername.class);

    public static void init() {
        // Togglz is calculating a hash value from the username which is then normalized to a value between 0 and 100
        // Considering we're starting from 0-100 value, this is an unnecessary overhead. A price we need to pay for using Togglz
        // to do load balancing...
        String username = new Integer(random.nextInt(100)).toString();
        log.debug("Generated username="+username);
        threadLocalUsername.set(username);
    }

    public static String get() {
        return threadLocalUsername.get();
    }

    public static void remove() {
        threadLocalUsername.remove();
    }

}
