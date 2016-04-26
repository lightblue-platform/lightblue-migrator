package com.redhat.lightblue.migrator;

import java.util.List;
import java.util.Collection;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import com.redhat.lightblue.client.response.LightblueResponse;

public class InterruptibleStuckMigrator extends  Migrator {

    private static final Logger LOGGER=LoggerFactory.getLogger(InterruptibleStuckMigrator.class);
    public static int numInterrupted=0;

    public InterruptibleStuckMigrator(ThreadGroup g) {
        super(g);
    }
    
    public List<JsonNode> getSourceDocuments() {
        LOGGER.debug("getSourceDocuments start");
        Breakpoint.checkpoint("Migrator:getSourceDocuments");
        while(true) {
            LOGGER.debug("getSourceDocuments waiting");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                numInterrupted++;
                Breakpoint.checkpoint("Migrator:interrupted");
                LOGGER.debug("getSourceDocuments interrupt");
                throw new RuntimeException(e);
            }
        }
    }

    public List<JsonNode> getDestinationDocuments(Collection<Identity> docs) {
        return null;
    }

    public List<String> compareDocs(JsonNode source,JsonNode dest) {
        return null;
    }

    public List<LightblueResponse> save(List<JsonNode> docs) {
        return null;
    }

    public String createRangeQuery(Date d,Date e) {
        return null;
    }
}

