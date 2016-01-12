package com.redhat.lightblue.migrator;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Literal;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.Update;
import com.redhat.lightblue.client.request.data.DataUpdateRequest;
import com.redhat.lightblue.client.response.LightblueResponse;

public abstract class Migrator extends Thread {

    private Logger LOGGER;

    private AbstractController controller;
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

    public void setController(AbstractController c) {
        controller=c;
    }

    public AbstractController getController() {
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
        return Utils.getLightblueClient(configPath);
    }


    /**
     * Override this to change how identity fields are retrieved
     */
    public List<String> getIdentityFields() {
        return getMigrationConfiguration().getDestinationIdentityFields();
    }

    public void migrate(MigrationJobExecution execution) {
        try {
            initMigrator();
            LOGGER.debug("Retrieving source docs");
            sourceDocs=Utils.getDocumentIdMap(getSourceDocuments(),getIdentityFields());
            Breakpoint.checkpoint("Migrator:sourceDocs");
            LOGGER.debug("There are {} source docs:{}",sourceDocs.size(),migrationJob.getConfigurationName());
            LOGGER.debug("Retrieving destination docs");
            destDocs=Utils.getDocumentIdMap(getDestinationDocuments(sourceDocs.keySet()),getIdentityFields());
            Breakpoint.checkpoint("Migrator:destDocs");
            LOGGER.debug("sourceDocs={}, destDocs={}",sourceDocs.size(),destDocs.size());

            insertDocs=new HashSet<>();
            for(Identity id:sourceDocs.keySet()) {
                if(!destDocs.containsKey(id)) {
                    insertDocs.add(id);
                }
            }
            Breakpoint.checkpoint("Migrator:insertDocs");
            LOGGER.debug("There are {} docs to insert",insertDocs.size());

            LOGGER.debug("Comparing source and destination docs");
            rewriteDocs=new HashSet<>();
            for(Map.Entry<Identity,JsonNode> sourceEntry:sourceDocs.entrySet()) {
                JsonNode destDoc=destDocs.get(sourceEntry.getKey());
                if(destDoc!=null) {
                    List<Inconsistency> inconsistencies=Utils.compareDocs(sourceEntry.getValue(),destDoc,getMigrationConfiguration().getComparisonExclusionPaths());
                    if(inconsistencies!=null&&!inconsistencies.isEmpty()) {
                        rewriteDocs.add(sourceEntry.getKey());
                        // log as key=value to make parsing easy
                        // fields to log: config name, job id, dest entity name & version, id field names & values,
                        //list of inconsistent paths
                        LOGGER.warn("configurationName={} destinationEntityName={} destinationEntityVersion={} migrationJobId={} identityFields=\"{}\" identityFieldValues=\"{}\" inconsistentPaths=\"{}\" mismatchedValues=\"{}\"",
                                     getMigrationConfiguration().getConfigurationName(),
                                     getMigrationConfiguration().getDestinationEntityName(),
                                     getMigrationConfiguration().getDestinationEntityVersion(),
                                     migrationJob.get_id(),
                                     StringUtils.join(getIdentityFields(), ","),
                                     sourceEntry.getKey().toString(),
                                     Inconsistency.getPathList(inconsistencies),
                                     Inconsistency.getMismatchedValues(inconsistencies));
                    }
                }
            }
            Breakpoint.checkpoint("Migrator:rewriteDocs");
            LOGGER.debug("There are {} docs to rewrite: {}",rewriteDocs.size(),migrationJob.getConfigurationName());
            execution.setInconsistentDocumentCount(rewriteDocs.size());
            execution.setOverwrittenDocumentCount(rewriteDocs.size());
            execution.setConsistentDocumentCount(sourceDocs.size()-rewriteDocs.size());

            List<JsonNode> saveDocsList=new ArrayList<>();
            for(Identity id:insertDocs) {
                saveDocsList.add(sourceDocs.get(id));
            }
            // Bug workaround: lightblue save API uses _id to find the old doc, but at this point, saveDocsList have documents with no _id
            // So, we find the docs in destDocs using their unique identifier, get _id from them, and add it to the docs
            for(Identity id:rewriteDocs) {
                JsonNode sourceDoc=sourceDocs.get(id);
                JsonNode destDoc=destDocs.get(id);
                if(destDoc!=null) {
                    ((ObjectNode)sourceDoc).set("_id",((ObjectNode)destDoc).get("_id"));
                }
                saveDocsList.add(sourceDoc);
            }

            execution.setProcessedDocumentCount(sourceDocs.size());

            LOGGER.debug("There are {} docs to save: {}",saveDocsList.size(),migrationJob.getConfigurationName());
            try {
                List<LightblueResponse> responses=save(saveDocsList);
                LOGGER.info("source: {}, dest: {}, written: {}",sourceDocs.size(),destDocs.size(),saveDocsList.size());
            } catch (LightblueException ex) {
                LOGGER.error("Error during migration of {}:{}",migrationJob.getConfigurationName(),ex.getMessage());
                execution.setErrorMsg(ex.getMessage());
            }
            Breakpoint.checkpoint("Migrator:complete");

        } catch (Exception e) {
            LOGGER.error("Error during migration of {}:{}",migrationJob.getConfigurationName(),e);
            StringWriter strw=new StringWriter();
            e.printStackTrace(new PrintWriter(strw));
            execution.setErrorMsg(strw.toString());
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

    public abstract List<LightblueResponse> save(List<JsonNode> docs) throws LightblueException;

    public abstract String createRangeQuery(Date startDate,Date endDate);


    @Override
    public final void run() {
        LOGGER=LoggerFactory.getLogger(Migrator.class.getName()+"."+getMigrationConfiguration().getConfigurationName());

        // First update the migration job, mark its status as being
        // processed, so it doesn't show up in other controllers'
        // tasks lists
        lbClient=controller.getController().getLightblueClient();
        DataUpdateRequest updateRequest = new DataUpdateRequest("migrationJob", null);
        updateRequest.where(Query.withValue("_id",Query.eq, migrationJob.get_id()));
        updateRequest.returns(Projection.includeField("_id"));

        MigrationJobExecution execution=new MigrationJobExecution();
        execution.setOwnerName(getMigrationConfiguration().getConsistencyCheckerName());
        execution.setHostName(getController().getController().getMainConfiguration().getName());
        execution.setActiveExecutionId(activeExecution.get_id());
        execution.setActualStartDate(activeExecution.getStartTime());
        execution.setStatus(MigrationJob.STATE_ACTIVE);
        // State is active
        updateRequest.updates(Update.update(Update.set("status",MigrationJob.STATE_ACTIVE),
                                            // Add a new execution element
                                            Update.set("jobExecutions",Literal.emptyArray()),
                                            Update.append("jobExecutions",Literal.emptyObject()),
                                            // Owner name
                                            Update.set("jobExecutions.-1.ownerName",execution.getOwnerName()).
                                            // Host name
                                            more("jobExecutions.-1.hostName",execution.getHostName()).
                                            // Execution id
                                            more("jobExecutions.-1.activeExecutionId",execution.getActiveExecutionId()).
                                            // Start date
                                            more("jobExecutions.-1.actualStartDate",Literal.value(execution.getActualStartDate())).
                                            // Status
                                            more("jobExecutions.-1.status",MigrationJob.STATE_ACTIVE)));
        LOGGER.debug("Marking job {} as active",migrationJob.get_id());
        LightblueResponse response=null;
        try {
            LOGGER.debug("Req:{}",updateRequest.getBody());

            response = lbClient.data(updateRequest);
            // Do the migration
            migrate(execution);

            // If there is error, 'error' will contain a messages, otherwise it'll be null
            // Update the state
            updateRequest=new DataUpdateRequest("migrationJob",null);
            updateRequest.where(Query.withValue("_id",Query.eq,migrationJob.get_id()));
            updateRequest.returns(Projection.includeField("_id"));
            if(execution.getErrorMsg()!=null) {
                execution.setStatus(MigrationJob.STATE_FAILED);
            } else {
                execution.setStatus(MigrationJob.STATE_COMPLETED);
            }
            updateRequest.updates(Update.update(Update.set("status",execution.getStatus()),
                                                Update.forEach("jobExecutions",
                                                               Query.withValue("activeExecutionId",Query.eq,activeExecution.get_id()),
                                                               Update.set("status",execution.getStatus()).
                                                               more("errorMsg",execution.getErrorMsg()==null?"":execution.getErrorMsg()).
                                                               more("processedDocumentCount",execution.getProcessedDocumentCount()).
                                                               more("consistentDocumentCount",execution.getConsistentDocumentCount()).
                                                               more("inconsistentDocumentCount",execution.getInconsistentDocumentCount()).
                                                               more("overwrittenDocumentCount",execution.getOverwrittenDocumentCount()).
                                                               more("actualEndDate",Literal.value(new Date())))));

            response=lbClient.data(updateRequest);
        } catch (Exception e) {
            LOGGER.error("Cannot update job {}, {} response:{}",migrationJob.get_id(),e,response.getJson());
        }
        controller.unlock(activeExecution.get_id());
    }

    private String quote(String s) {
        return s==null?null:"\""+s+"\"";
    }

    private String escape(String s) {
        return JsonNodeFactory.instance.textNode(s).toString();
    }

}

