package com.redhat.lightblue.migrator.facade;

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
        this.start = new Date();
    }

    public long complete() {
        Date end = new Date();
        long callTookMS = end.getTime()-start.getTime();

        if (log.isDebugEnabled()) {
            log.debug(method+" call took "+ callTookMS + "ms");
        }

        return callTookMS;
    }

}
