package com.redhat.lightblue.migrator;

import java.io.InputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.commons.lang.StringUtils;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueClientConfiguration;
import com.redhat.lightblue.client.PropertiesLightblueClientConfiguration;
import com.redhat.lightblue.client.hystrix.LightblueHystrixClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.enums.ExpressionOperation;
import com.redhat.lightblue.client.enums.SortDirection;
import com.redhat.lightblue.client.response.LightblueResponse;
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataInsertRequest;
import com.redhat.lightblue.client.request.data.DataDeleteRequest;
import com.redhat.lightblue.client.request.data.DataUpdateRequest;
import com.redhat.lightblue.client.expression.update.SetUpdate;
import com.redhat.lightblue.client.expression.update.PathValuePair;
import com.redhat.lightblue.client.expression.update.AppendUpdate;
import com.redhat.lightblue.client.expression.update.LiteralRValue;
import com.redhat.lightblue.client.expression.update.ObjectRValue;
import com.redhat.lightblue.client.expression.update.ForeachUpdate;
import com.redhat.lightblue.client.expression.update.Update;
import com.redhat.lightblue.client.util.ClientConstants;

import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;
import static com.redhat.lightblue.client.projection.FieldProjection.includeField;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.and;

public abstract class Migrator extends Thread {

    private static final Logger LOGGER=LoggerFactory.getLogger(Migrator.class);

    private MigratorController controller;
    private MigrationJob migrationJob;
    private ActiveExecution activeExecution;

    private LightblueClient lbClient;


    // Migration context, observable by tests
    private Map<Identity,JsonNode> sourceDocs;
    private Map<Identity,JsonNode> destDocs;
    private Set<Identity> insertDocs;
    private Set<Identity> rewriteDocs;

    public Migrator(ThreadGroup grp) {
        super(grp,"Migrator");
    }

    public Map<Identity,JsonNode> getSourceDocs() {
        return sourceDocs;
    }

    public Map<Identity,JsonNode> getDestDocs() {
        return destDocs;
    }

    public Set<Identity> getInsertDocs() {
        return insertDocs;
    }

    public Set<Identity> getRewriteDocs() {
        return rewriteDocs;
    }
    
    public void setController(MigratorController c) {
        this.controller=c;
    }

    public MigratorController getController() {
        return controller;
    }

    public MigrationConfiguration getMigrationConfiguration() {
        return controller.getMigrationConfiguration();
    }

    public void setMigrationJob(MigrationJob m) {
        migrationJob=m;
    }

    public void setActiveExecution(ActiveExecution e) {
        activeExecution=e;
    }

    public MigrationJob getMigrationJob() {
        return migrationJob;
    }

    public ActiveExecution getActiveExecution() {
        return activeExecution;
    }

    public LightblueClient getLightblueClient(String configPath)
        throws IOException {
        LightblueClient cli;
        if (configPath == null) {
            cli = new LightblueHttpClient();
        } else {
            try (InputStream is = controller.getContextClassLoader().getResourceAsStream(configPath)) {
                LightblueClientConfiguration config = PropertiesLightblueClientConfiguration.fromInputStream(is);
                cli = new LightblueHttpClient(config);
            }
        }       
        return new LightblueHystrixClient(cli, "migrator", "cli");
    }
    
    /**
     * Updates active execution numDocsProcessed and numDocsToPRocess values, and ping time
     */
    public void updateActiveExecution(Integer numDocsProcessed, Integer numDocsToProcess) {
        if(lbClient!=null) {
            DataUpdateRequest req=new DataUpdateRequest("activeExecution",null);
            req.where(withValue("_id",ExpressionOperation.EQ,activeExecution.get_id()));
            req.returns(includeField("_id"));
            List<Update> updates=new ArrayList<>();
            updates.add(new SetUpdate(new PathValuePair("ping",new LiteralRValue(ClientConstants.getDateFormat().format(activeExecution.getStartTime())))));
            if(numDocsProcessed!=null)
                updates.add(new SetUpdate(new PathValuePair("numDocsProcessed",new LiteralRValue(numDocsProcessed.toString()))));
            if(numDocsToProcess!=null)
                updates.add(new SetUpdate(new PathValuePair("numDocsToProcess",new LiteralRValue(numDocsToProcess.toString()))));
            req.setUpdates(updates);
            lbClient.data(req);
        } else
            throw new IllegalStateException();
    }

    public void migrate(MigrationJobExecution execution) {
        try {
            initMigrator();
            LOGGER.debug("Retrieving source docs");
            sourceDocs=getDocumentIdMap(getSourceDocuments());
            Breakpoint.checkpoint("Migrator:sourceDocs");
            LOGGER.debug("There are {} source docs",sourceDocs.size());
            LOGGER.debug("Retrieving destination docs");
            destDocs=getDocumentIdMap(getDestinationDocuments(sourceDocs.keySet()));
            Breakpoint.checkpoint("Migrator:destDocs");
            LOGGER.debug("There are {} destination docs",destDocs.size());

            insertDocs=new HashSet<>();
            for(Identity id:sourceDocs.keySet())
                if(!destDocs.containsKey(id))
                    insertDocs.add(id);
            Breakpoint.checkpoint("Migrator:insertDocs");
            LOGGER.debug("There are {} docs to insert",insertDocs.size());
            
            LOGGER.debug("Comparing source and destination docs");
            rewriteDocs=new HashSet<>();
            for(Map.Entry<Identity,JsonNode> sourceEntry:sourceDocs.entrySet()) {
                JsonNode destDoc=destDocs.get(sourceEntry.getKey());
                if(destDoc!=null) {
                    List<String> inconsistentFields=compareDocs(sourceEntry.getValue(),destDoc);
                    if(inconsistentFields!=null&&!inconsistentFields.isEmpty()) {
                        rewriteDocs.add(sourceEntry.getKey());
                        // log as key=value to make parsing easy
                        // fields to log: config name, job id, dest entity name & version, id field names & values,
                        //list of inconsistent paths
                        LOGGER.error("configurationName={} destinationEntityName={} destinationEntityVersion={} migrationJobId={} identityFields=\"{}\" identityFieldValues=\"{}\" inconsistentPaths=\"{}\"",
                                     getMigrationConfiguration().getConfigurationName(),
                                     getMigrationConfiguration().getDestinationEntityName(),
                                     getMigrationConfiguration().getDestinationEntityVersion(),
                                     migrationJob.get_id(),
                                     StringUtils.join(getMigrationConfiguration().getDestinationIdentityFields(), ","),
                                     sourceEntry.getKey().toString(),
                                     StringUtils.join(inconsistentFields, ","));
                    }
                }
            }
            Breakpoint.checkpoint("Migrator:rewriteDocs");
            LOGGER.debug("There are {} docs to rewrite",rewriteDocs.size());
            execution.setInconsistentDocumentCount(rewriteDocs.size());
            execution.setOverwrittenDocumentCount(rewriteDocs.size());
            execution.setConsistentDocumentCount(sourceDocs.size()-rewriteDocs.size());

            List<JsonNode> saveDocsList=new ArrayList<>();
            for(Identity id:insertDocs)
                saveDocsList.add(sourceDocs.get(id));
            for(Identity id:rewriteDocs)
                saveDocsList.add(sourceDocs.get(id));

            execution.setProcessedDocumentCount(sourceDocs.size());
            
            LOGGER.debug("There are {} docs to save",saveDocsList.size());
            save(saveDocsList);
            LOGGER.debug("Docs saved: {}",saveDocsList.size());
            Breakpoint.checkpoint("Migrator:complete");

        } catch (Exception e) {
            LOGGER.error("Error during migration:{}",e);
            execution.setErrorMsg(e.toString());
        } finally {
            cleanupMigrator();
        }
    }

    /**
     * Notifies the implementing class that processing has started
     */
    public void initMigrator() {}

    /**
     * Notifies the implementing class that processing has finished
     */
    public void cleanupMigrator() {}

    /**
     * Should return a list of source documents
     */
    public abstract List<JsonNode> getSourceDocuments();

    /**
     * Should return a list of destination documents
     */
    public abstract List<JsonNode> getDestinationDocuments(Collection<Identity> docs);

    /**
     * Should compare two docs, and return mismatched fields
     */
    public abstract List<String> compareDocs(JsonNode source,JsonNode dest);

    public abstract List<LightblueResponse> save(List<JsonNode> docs);

    /**
     * Build an id-doc map from a list of docs
     */
    public Map<Identity,JsonNode> getDocumentIdMap(List<JsonNode> list) {
        Map<Identity,JsonNode> map=new HashMap<>();
        if(list!=null)
            for(JsonNode node:list) {
                map.put(new Identity(node, getMigrationConfiguration().getDestinationIdentityFields()),node);
            }
        return map;
    }
    
    
    @Override
    public final void run() {
        // First update the migration job, mark its status as being
        // processed, so it doesn't show up in other controllers'
        // tasks lists
        lbClient=controller.getController().getLightblueClient();
        DataUpdateRequest updateRequest = new DataUpdateRequest("migrationJob", null);
        updateRequest.where(withValue("_id",ExpressionOperation.EQ, migrationJob.get_id()));
        updateRequest.returns(includeField("_id"));

        MigrationJobExecution execution=new MigrationJobExecution();
        execution.setOwnerName(getMigrationConfiguration().getConsistencyCheckerName());
        execution.setHostName(getController().getController().getMainConfiguration().getName());
        execution.setActiveExecutionId(activeExecution.get_id());
        execution.setActualStartDate(activeExecution.getStartTime());
        execution.setStatus(MigrationJob.STATE_ACTIVE);
        // State is active
        updateRequest.updates(new SetUpdate(new PathValuePair("status",new LiteralRValue(quote(MigrationJob.STATE_ACTIVE)))),
                              // Add a new execution element
                              new AppendUpdate("jobExecutions",new ObjectRValue(new HashMap())),
                              // Owner name
                              new SetUpdate(new PathValuePair("jobExecutions.-1.ownerName",new LiteralRValue(quote(execution.getOwnerName())))),
                              // Host name
                              new SetUpdate(new PathValuePair("jobExecutions.-1.hostName",new LiteralRValue(quote(execution.getHostName())))),
                              // Execution id
                              new SetUpdate(new PathValuePair("jobExecutions.-1.activeExecutionId",new LiteralRValue(quote(execution.getActiveExecutionId())))),
                              // Start date
                              new SetUpdate(new PathValuePair("jobExecutions.-1.actualStartDate",new LiteralRValue(quote(ClientConstants.getDateFormat().format(execution.getActualStartDate()))))),
                              // Status
                              new SetUpdate(new PathValuePair("jobExecutions.-1.status",new LiteralRValue(quote(MigrationJob.STATE_ACTIVE)))));
        LOGGER.debug("Marking job {} as active",migrationJob.get_id());
        LightblueResponse response=null;
        try {
            LOGGER.debug("Req:{}",updateRequest.getBody());
                         
            response = lbClient.data(updateRequest);
            if(!response.hasError()) {
                // Do the migration
                migrate(execution);

                // If there is error, 'error' will contain a messages, otherwise it'll be null
                // Update the state
                updateRequest=new DataUpdateRequest("migrationJob",null);
                updateRequest.where(withValue("_id = "+migrationJob.get_id()));
                updateRequest.returns(includeField("_id"));
                if(execution.getErrorMsg()!=null)
                    execution.setStatus(MigrationJob.STATE_FAILED);
                else
                    execution.setStatus(MigrationJob.STATE_COMPLETED);
                updateRequest.updates(new SetUpdate(new PathValuePair("status",new LiteralRValue(quote(execution.getStatus())))),
                                      new ForeachUpdate("jobExecutions",
                                                        withValue("activeExecutionId",ExpressionOperation.EQ,activeExecution.get_id()),
                                                        new SetUpdate(new PathValuePair("status",new LiteralRValue(quote(execution.getStatus()))),
                                                                      new PathValuePair("errorMsg",new LiteralRValue(quote(execution.getErrorMsg()==null?"":execution.getErrorMsg()))),
                                                                      new PathValuePair("processedDocumentCount",new LiteralRValue(Integer.toString(execution.getProcessedDocumentCount()))),
                                                                      new PathValuePair("consistentDocumentCount",new LiteralRValue(Integer.toString(execution.getConsistentDocumentCount()))),
                                                                      new PathValuePair("inconsistentDocumentCount",new LiteralRValue(Integer.toString(execution.getInconsistentDocumentCount()))),
                                                                      new PathValuePair("overwrittenDocumentCount",new LiteralRValue(Integer.toString(execution.getOverwrittenDocumentCount()))),
                                                                      new PathValuePair("actualEndDate", new LiteralRValue(quote(ClientConstants.getDateFormat().format(new Date())))))));

                response=lbClient.data(updateRequest);
                if(response.hasError())
                    throw new RuntimeException("Failed to update:"+response);

            } else
                throw new RuntimeException("Failed to update:"+response);
        } catch (Exception e) {
            LOGGER.error("Cannot update job {}, {} response:{}",migrationJob.get_id(),e,response.getJson());
        }
    }

    private String quote(String s) {
        return s==null?null:"\""+s+"\"";
    }

}

