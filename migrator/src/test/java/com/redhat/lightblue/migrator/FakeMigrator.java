package com.redhat.lightblue.migrator;

import java.util.List;
import java.util.Collection;
import java.util.Date;

import com.fasterxml.jackson.databind.JsonNode;

import com.redhat.lightblue.client.response.LightblueResponse;

public class FakeMigrator extends Migrator {

    public static int count;

    public FakeMigrator(ThreadGroup g) {
        super(g);
    }

    public void migrate(MigrationJobExecution execution) {
        System.out.println("Testmigrator ran");
        count++;
    }

    public List<JsonNode> getSourceDocuments() {
        return null;
    }

    public List<JsonNode> getDestinationDocuments(Collection<Identity> docs) {
        return null;
    }

    public List<String> compareDocs(JsonNode source, JsonNode dest) {
        return null;
    }

    public List<LightblueResponse> save(List<JsonNode> docs) {
        return null;
    }

    public String createRangeQuery(Date d, Date e) {
        return d.toString() + "->" + e.toString();
    }
}
