package com.redhat.lightblue.migrator;

import java.util.List;
import java.util.Collection;

import com.fasterxml.jackson.databind.JsonNode;

import com.redhat.lightblue.client.response.LightblueResponse;

public class TestMigrator extends Migrator {

    public static int count;
    
    public String migrate() {
        count++;
        return null;
    }

    public List<JsonNode> getSourceDocuments() {
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

