package com.redhat.lightblue.migrator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

public class TestMigrator extends DefaultMigrator {

    public Map<Identity, JsonNode> sourceDocs, destDocs;
    public Set<Identity> insertDocs, rewriteDocs;
    public List<JsonNode> saveDocsList;

    public TestMigrator(ThreadGroup grp) {
        super(grp);
    }

    @Override
    protected void beforeSaveToDestination(Map<Identity, JsonNode> sourceDocs, Map<Identity, JsonNode> destDocs, Set<Identity> insertDocs,
            Set<Identity> rewriteDocs, List<JsonNode> saveDocsList) {

        this.sourceDocs = sourceDocs;
        this.destDocs = destDocs;
        this.insertDocs = insertDocs;
        this.rewriteDocs = rewriteDocs;
        this.saveDocsList = saveDocsList;
    }

}
