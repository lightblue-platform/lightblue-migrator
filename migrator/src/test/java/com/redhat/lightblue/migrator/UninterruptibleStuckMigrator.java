package com.redhat.lightblue.migrator;

import java.util.List;
import java.util.Collection;
import java.util.Date;

import com.fasterxml.jackson.databind.JsonNode;

import com.redhat.lightblue.client.response.LightblueResponse;

public class UninterruptibleStuckMigrator extends  Migrator {

    public UninterruptibleStuckMigrator(ThreadGroup g) {
        super(g);
    }
    
    public List<JsonNode> getSourceDocuments() {
        Breakpoint.checkpoint("Migrator:getSourceDocuments");
        while(true);
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

