package com.redhat.lightblue.migrator.utils;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class to measure method execution times.
 *
 * @author mpatercz
 *
 */
public class Timer {

    private static final Logger log = LoggerFactory.getLogger(Timer.class);

    public final String method;

    public final Date start;

    public Timer(String method) {
        super();
        this.method = method;
        this.start = log.isDebugEnabled() ? new Date(): null;
    }

    public void complete() {
        if (log.isDebugEnabled()) {
            Date end = new Date();
            log.debug(method+" call took "+ (end.getTime()-start.getTime())+"ms");
        }
    }

}
