package com.redhat.lightblue.migrator;

import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Works like a breakpoint in a debugger. If stop is called, execution
 * stops when run() is called until resume() is called by another
 * thread.
 */
public class Breakpoint {

    /**
     * true is the breakpoint will stop execution
     */
    private volatile boolean stopped=false;

    /**
     * This is set to true at checkpoint()
     */
    private volatile boolean ran=false;

    /**
     * true if waitUntil() is called
     */
    private boolean waiting=false;
    private final String name;

    private static final Map<String,Breakpoint> bps=new HashMap<>();

    public Breakpoint() {
        name=null;
    }

    public Breakpoint(String name) {
        this.name=name;
    }

    public String getName() {
        return name;
    }
    
    public void stop() {
        stopped=true;
        ran=false;
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
    
    public void checkpoint() {
        if(stopped) {
            synchronized(this) {
                if(stopped) {
                    ran=true;
                    notify();
                    try {
                        wait();
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

    public static void stop(String bp) {
        get(bp).stop();
    }

    public static void resume(String bp) {
        get(bp).resume();
    }

    public static void waitUntil(String bp) {
        get(bp).waitUntil();
    }

    public static void checkpoint(String bp) {
        get(bp).checkpoint();
    }

    private static Breakpoint get(String bp) {
        Breakpoint x=bps.get(bp);
        if(x==null)
            bps.put(bp,x=new Breakpoint(bp));
        return x;
    }
}
