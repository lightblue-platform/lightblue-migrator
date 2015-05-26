package com.redhat.lightblue.migrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Works like a breakpoint in a debugger. If stop is called, execution
 * stops when run() is called until resume() is called by another
 * thread.
 */
public class Breakpoint {

    private static final Logger LOGGER=LoggerFactory.getLogger(Breakpoint.class);

    private volatile boolean stopped=false;
    private volatile boolean ran=false;
    private boolean waiting=false;

    public void stop() {
        stopped=true;
    }

    public void resume() {
        if(stopped) {
            synchronized(this) {
                stopped=false;
                notify();
            }
        }
    }

    public void waitUntil() {
        if(!ran) {
            synchronized(this) {
                if(!ran) {
                    waiting=true;
                    try {
                        wait();
                        ran=false;
                    } catch (Exception e) {}
                    waiting=false;
                }
            }
        }
    }

    public void run() {
        run(null);
    }
    
    public void run(String msg) {
        if(stopped) {
            synchronized(this) {
                if(stopped) {
                    if(msg!=null)
                        LOGGER.debug("Waiting: {}",msg);
                    else
                        LOGGER.debug("Waiting...");
                    ran=true;
                    notify();
                    try {
                        wait();
                        if(msg!=null)
                            LOGGER.debug("Resumed: {}",msg);
                        else
                            LOGGER.debug("Resumed");
                    } catch (Exception e) {}
                } 
            }
        } else {
            if(waiting) {
                synchronized(this) {
                    ran=true;
                    notify();
                }
            }
        }
    }
}
