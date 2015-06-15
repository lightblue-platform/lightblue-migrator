package com.redhat.lightblue.migrator;

import java.util.List;
import java.util.Collection;

import com.fasterxml.jackson.databind.JsonNode;

import com.redhat.lightblue.client.response.LightblueResponse;

public class FailMigrator extends  Migrator {

    public FailMigrator(ThreadGroup g) {
        super(g);
    }
    
    public List<JsonNode> getSourceDocuments() {
        // Throw exception here
        String x=null;
        x.charAt(0);
        return null;
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
}

