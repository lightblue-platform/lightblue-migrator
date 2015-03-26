package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.and;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.or;
import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.*;

import java.io.IOException;
import java.text.DateFormat;
import java.util.*;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.enums.SortDirection;
import com.redhat.lightblue.client.expression.query.Query;
import com.redhat.lightblue.client.expression.query.ValueQuery;
import com.redhat.lightblue.client.expression.update.AppendUpdate;
import com.redhat.lightblue.client.expression.update.ObjectRValue;
import com.redhat.lightblue.client.expression.update.PathValuePair;
import com.redhat.lightblue.client.expression.update.SetUpdate;
import com.redhat.lightblue.client.expression.update.Update;
import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.hystrix.LightblueHystrixClient;
import com.redhat.lightblue.client.projection.FieldProjection;
import com.redhat.lightblue.client.projection.Projection;
import com.redhat.lightblue.client.request.LightblueRequest;
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataSaveRequest;
import com.redhat.lightblue.client.request.data.DataUpdateRequest;
import com.redhat.lightblue.client.response.LightblueResponse;
import com.redhat.lightblue.client.response.LightblueResponseParseException;
import com.redhat.lightblue.client.util.ClientConstants;
import java.sql.SQLException;
import java.util.List;

public class MigrationJob implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationJob.class);

    /**
     * How long an executing job runs before we decide it's dead and mark that
     * execution as complete. Currently 60 minutes.
     */
    private static final int JOB_EXECUTION_TIMEOUT_MSEC = 60 * 60 * 1000;

    protected static final int BATCH_SIZE = 100;

    protected static final ObjectMapper mapper = new ObjectMapper()
            .setDateFormat(ClientConstants.getDateFormat())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    public static final String STATUS = "status";
    public static final String COMPLETE = "complete";
    public static final String PARTIAL = "partial";
    public static final String ASYNC = "async";
    public static final String ERROR = "error";

    public MigrationJob() {
        migrationConfiguration = new MigrationConfiguration();
    }

    public MigrationJob(MigrationConfiguration migrationConfiguration) {
        this.migrationConfiguration = migrationConfiguration;
    }

    private String sourceConfigPath;
    private String destinationConfigPath;

    private LightblueClient sourceClient;
    private LightblueClient destinationClient;

    // configuration for migrator
    private MigrationConfiguration migrationConfiguration;

    private List<MigrationJobExecution> jobExecutions;

    MigrationJobExecution currentRun;

    private String _id;

    // information about migrator instance working job
    private String owner;
    private String hostName;
    private String pid;

    // dates of data to migrate
    private Date startDate;
    private Date endDate;

    // when this job can be worked by migrator
    private Date whenAvailableDate;

    // how long we think it will take
    private int expectedExecutionMilliseconds;

    private boolean hasInconsistentDocuments = false;

    private List<Map.Entry<DataSaveRequest, LightblueResponse>> overwriteProcessedList = new ArrayList<>();

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public MigrationConfiguration getJobConfiguration() {
        return migrationConfiguration;
    }

    public void setJobConfiguration(MigrationConfiguration jobConfiguration) {
        this.migrationConfiguration = jobConfiguration;
    }

    protected List<MigrationJobExecution> getJobExecutions() {
        if (null == jobExecutions) {
            jobExecutions = new ArrayList<>(1);
        }
        return jobExecutions;
    }

    public void setJobExecutions(List<MigrationJobExecution> jobExecutions) {
        this.jobExecutions = jobExecutions;
    }

    public LightblueClient getSourceClient() {
        return sourceClient;
    }

    public void setSourceClient(LightblueClient client) {
        this.sourceClient = client;
    }

    public LightblueClient getDestinationClient() {
        return destinationClient;
    }

    public void setDestinationClient(LightblueClient client) {
        this.destinationClient = client;
    }

    public void setOverwriteDestinationDocuments(boolean overwriteDestinationDocuments) {
        migrationConfiguration.setOverwriteDestinationDocuments(overwriteDestinationDocuments);
    }

    public boolean shouldOverwriteDestinationDocuments() {
        return migrationConfiguration.shouldOverwriteDestinationDocuments();
    }

    public String getSourceConfigPath() {
        return sourceConfigPath;
    }

    public void setSourceConfigPath(String configPath) {
        this.sourceConfigPath = configPath;
    }

    public String getDestinationConfigPath() {
        return destinationConfigPath;
    }

    public void setDestinationConfigPath(String configPath) {
        this.destinationConfigPath = configPath;
    }

    /**
     * Returns true if there are any inconsistent documents
     *
     * @return true if there are any inconsistent documents
     */
    public boolean hasInconsistentDocuments() {
        return hasInconsistentDocuments;
    }

    public String getConfigurationName() {
        return migrationConfiguration.getConfigurationName();
    }

    public void setConfigurationName(String configurationName) {
        this.migrationConfiguration.setConfigurationName(configurationName);
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public Date getWhenAvailableDate() {
        return whenAvailableDate;
    }

    public void setWhenAvailableDate(Date whenAvailable) {
        this.whenAvailableDate = whenAvailable;
    }

    public int getExpectedExecutionMilliseconds() {
        return expectedExecutionMilliseconds;
    }

    public void setExpectedExecutionMilliseconds(int expectedExecutionMilliseconds) {
        this.expectedExecutionMilliseconds = expectedExecutionMilliseconds;
    }

    public int getDocumentsProcessed() {
        return currentRun.getProcessedDocumentCount();
    }

    public int getConsistentDocuments() {
        return currentRun.getConsistentDocumentCount();
    }

    public int getInconsistentDocuments() {
        return currentRun.getInconsistentDocumentCount();
    }

    public int getRecordsOverwritten() {
        return currentRun.getOverwrittenDocumentCount();
    }

    @Override
    public void run() {
        LOGGER.debug("MigrationJob started");
        int jobExecutionPsn = -1;

        try {
            currentRun = new MigrationJobExecution();
            currentRun.setOwnerName(owner);
            currentRun.setHostName(hostName);
            currentRun.setPid(pid);
            currentRun.setActualStartDate(new Date());
            overwriteProcessedList = new ArrayList<>();
            getJobExecutions().add(currentRun);

            configureClients();

            LightblueResponse response = saveJobDetails(-1);
            LOGGER.debug("Start Save Response: {}", response.getText());

            Object[] x = shouldProcessJob(response);
            boolean processJob = (Boolean) x[0];
            jobExecutionPsn = (Integer) x[1];

            if (processJob) {
                currentRun.setJobStatus(JobStatus.RUNNING);
                LightblueResponse responseMarkExecutionStatus = markExecutionStatusAndEndDate(jobExecutionPsn, currentRun.getJobStatus(), false);
                LOGGER.debug("Updated Status Response for status {}: {}", currentRun.getJobStatus(), responseMarkExecutionStatus.getText());

                Map<String, JsonNode> sourceDocuments = getSourceDocuments();
                Map<String, JsonNode> destinationDocuments = getDestinationDocuments(sourceDocuments);


                /*
                 Response can be (according from the documentation http://jewzaam.gitbooks.io/lightblue-specifications/content/language_specification/data.html ):
                 complete: The operation completed, all requests are processed
                 partial: Some of the operations are successful, some failed
                 async: Operation running asynchronously
                 error: Operation failed because of some system error
                 */
                boolean allComplete = true;
                boolean anyPartial = false;
                boolean anyAsync = false;
                for (JsonNode jsonNode : sourceDocuments.values()) {
                    JsonNode jsStatus = jsonNode.findValue(STATUS);
                    if (jsStatus == null || jsStatus instanceof NullNode || jsStatus instanceof MissingNode) {
                        // It should not reach here as is expected lightblue to throw an exception
                        LOGGER.error("In sourceDocuments: Found 'error' in response's status in one of the documents!");
                        throw new RuntimeException("Found 'error' in response's status!");
                    }
                    String status = jsStatus.asText(); // TODO lightblue client should have a better way to handle this
                    if (COMPLETE.equalsIgnoreCase(status)) {
                        continue;
                    } else {
                        allComplete = false;
                        if (PARTIAL.equalsIgnoreCase(status)) {
                            anyPartial = true;
                        } else if (ASYNC.equalsIgnoreCase(status)) {
                            anyAsync = true;
                        } else if (ERROR.equalsIgnoreCase(status)) {
                            // It should not reach here as is expected lightblue to throw an exception
                            LOGGER.error("In sourceDocuments: Found 'error' in response's status in one of the documents!");
                            throw new RuntimeException("Found 'error' in response's status!");
                        } else {
                            // It should not reach here as lightblue response should always have a status
                            LOGGER.error("In sourceDocuments: One of the JSON returned doesn't have a valid status. Status set on the response was \"{}\"", status);
                            throw new RuntimeException("Invalid status from the response: " + status);
                        }
                    }
                }
                for (JsonNode jsonNode : destinationDocuments.values()) {
                    String status = jsonNode.findValue("status").asText();
                    if (COMPLETE.equalsIgnoreCase(status)) {
                        continue;
                    } else {
                        allComplete = false;
                        if (PARTIAL.equalsIgnoreCase(status)) {
                            anyPartial = true;
                        } else if (ASYNC.equalsIgnoreCase(status)) {
                            anyAsync = true;
                        } else if (ERROR.equalsIgnoreCase(status)) {
                            // It should not reach here as is expected lightblue to throw an exception
                            LOGGER.error("In destinationDocuments: Found 'error' in response's status in one of the documents!");
                            throw new RuntimeException("Found 'error' in response's status!");
                        } else {
                            // It should not reach here as lightblue response should always have a status
                            LOGGER.error("In destinationDocuments: One of the JSON returned doesn't have a valid status. Status set on the response was \"{}\"", status);
                            throw new RuntimeException("Invalid status from the response: " + status);
                        }
                    }
                }
                // if any async, update the status of the job
                if (anyAsync) {
                    currentRun.setJobStatus(JobStatus.RUNNING_ASYNC);
                    responseMarkExecutionStatus = markExecutionStatusAndEndDate(jobExecutionPsn, currentRun.getJobStatus(), false);
                    LOGGER.debug("Updated Status Response for status {}: {}", currentRun.getJobStatus(), responseMarkExecutionStatus.getText());
                }

                List<JsonNode> documentsToOverwrite = getDocumentsToOverwrite(sourceDocuments, destinationDocuments);
                if (!documentsToOverwrite.isEmpty()) {
                    hasInconsistentDocuments = true;
                }

                currentRun.setProcessedDocumentCount(sourceDocuments.size());
                currentRun.setConsistentDocumentCount(sourceDocuments.size() - documentsToOverwrite.size());
                currentRun.setInconsistentDocumentCount(documentsToOverwrite.size());

                if (shouldOverwriteDestinationDocuments() && hasInconsistentDocuments) {
                    currentRun.setOverwrittenDocumentCount(overwriteLightblue(documentsToOverwrite));

                    // Check the status of the overwritten documents
                    allComplete = true;
                    anyPartial = false;
                    anyAsync = false;

                    for (Entry<DataSaveRequest, LightblueResponse> entry : overwriteProcessedList) {
                        JsonNode jsonNode = entry.getValue().getJson();
                        JsonNode jsStatus = jsonNode.findValue("status");
                        if (jsStatus == null || jsStatus instanceof NullNode || jsStatus instanceof MissingNode) {
                            // It should not reach here as is expected lightblue to throw an exception
                            LOGGER.error("In sourceDocuments: Found 'error' in response's status in one of the documents!");
                            throw new RuntimeException("Found 'error' in response's status!");
                        }
                        String status = jsStatus.asText(); // TODO lightblue client should have a better way to handle this
                        if ("complete".equalsIgnoreCase(status)) {
                            continue;
                        } else {
                            allComplete = false;
                            if ("partial".equalsIgnoreCase(status)) {
                                anyPartial = true;
                            } else if ("anyAsync".equalsIgnoreCase(status)) {
                                anyAsync = true;
                            } else if ("error".equalsIgnoreCase(status)) {
                                // It should not reach here as is expected lightblue to throw an exception
                                LOGGER.error("In sourceDocuments: Found 'error' in response's status in one of the documents!");
                                throw new RuntimeException("Found 'error' in response's status!");
                            } else {
                                // It should not reach here as lightblue response should always have a status
                                LOGGER.error("In sourceDocuments: One of the JSON returned doesn't have a valid status. Status set on the response was \"{}\"", status);
                                throw new RuntimeException("Invalid status from the response: " + status);
                            }
                        }
                    }
                }
                if (allComplete) {
                    currentRun.setJobStatus(JobStatus.COMPLETED_SUCCESS);
                } else if (anyPartial) {
                    currentRun.setJobStatus(JobStatus.COMPLETED_PARTIAL);
                } else if (anyAsync) {
                    currentRun.setJobStatus(JobStatus.COMPLETED_IGNORED);
                } else {
                    currentRun.setJobStatus(JobStatus.UNKNOWN);
                }

                currentRun.setActualEndDate(new Date());
                saveJobDetails(jobExecutionPsn);
                LOGGER.debug("Success Save Response: {}", response.getText());
            } else {
                // just mark complete and set actual end date.
                LightblueResponse responseMarkExecutionStatus = markExecutionStatusAndEndDate(jobExecutionPsn, currentRun.getJobStatus(), true);
                LOGGER.debug("No Run Mark Updated Response: {}", responseMarkExecutionStatus.getText());
            }

        } catch (RuntimeException | LightblueResponseParseException | SQLException | IOException e) {
            // would be nice to reference DataType.DATE_FORMAT_STR in core..
            DateFormat dateFormat = ClientConstants.getDateFormat();
            LOGGER.error(String.format("Error while processing job %s with %s for start date %s and end date %s",
                    _id,
                    getJobConfiguration(), (getStartDate() == null ? "null" : dateFormat.format(getStartDate())),
                    (getEndDate() == null ? "null" : dateFormat.format(getEndDate()))), e);
            try {
                currentRun.setJobStatus(JobStatus.ABORTED_UNKNOWN);
                LightblueResponse lr = markExecutionStatusAndEndDate(jobExecutionPsn, currentRun.getJobStatus(), true);
                LOGGER.debug("Processing RuntimeException and just updated Status Response for status {}: {}", currentRun.getJobStatus(), lr.getText());
            } catch (RuntimeException re) {
                LOGGER.error("Couldn't update failed job's status", re);
            }
        }

        LOGGER.debug("MigrationJob completed");
    }

    /**
     * Return array with two elements indicating if the job should process and
     * what the index of the jobExecution is.
     *
     * @param response
     * @return object array where index 0 is a Boolean indicating if the job
     * should be processed, index 1 is an Integer indicating the position in the
     * jobExecutions array for this job execution attempt
     */
    private Object[] shouldProcessJob(LightblueResponse response) throws LightblueResponseParseException {
        boolean processJob = true;
        int jobExecutionPsn = -1;

        MigrationJob[] jobs = response.parseProcessed(MigrationJob[].class);

        // should only be one job returned, verify
        if (jobs.length > 1) {
            throw new RuntimeException("Error parsing lightblue response: more than one job returned: " + jobs.length);
        }

        // first incomplete job execution must be for this execution else we don't process it
        int i = 0;
        for (MigrationJobExecution execution : jobs[0].getJobExecutions()) {
            // if we find an execution that is not our pid but is active
            // in the array before ours, we do not get to process
            if (!execution.getJobStatus().isCompleted() && !pid.equals(execution.getPid())) {
                // check if this is a dead job
                if (execution.getActualStartDate() != null
                        && System.currentTimeMillis() - execution.getActualStartDate().getTime() > JOB_EXECUTION_TIMEOUT_MSEC) {
                    // job is dead, mark it complete
                    LightblueResponse responseMarkDead = markExecutionStatusAndEndDate(i, JobStatus.COMPLETED_DEAD, true);
                    LOGGER.debug("Response is dead update: {}", responseMarkDead.getText());
                } else {
                    // we're not the one processing this guy!
                    processJob = false;
                }
            }

            // find our job's execution in the array
            // once we find our job, we can stop looping, we've decided
            // already if this execution gets to be processed or not.
            if (pid.equals(execution.getPid())) {
                jobExecutionPsn = i;
                break;
            }

            // increment position in array
            i++;
        }

        return new Object[]{processJob, jobExecutionPsn};
    }

    private void configureClients() {
        LightblueHttpClient source;
        LightblueHttpClient destination;
        if (getSourceConfigPath() == null && getDestinationConfigPath() == null) {
            source = new LightblueHttpClient();
            destination = new LightblueHttpClient();
        } else {
            source = new LightblueHttpClient(getSourceConfigPath());
            destination = new LightblueHttpClient(getDestinationConfigPath());
        }
        sourceClient = new LightblueHystrixClient(source, "migrator", "sourceClient");
        destinationClient = new LightblueHystrixClient(destination, "migrator", "destinationClient");
    }

    private LightblueResponse markExecutionStatusAndEndDate(int jobExecutionPsn, JobStatus jobStatus, boolean updateEndDate) {
        // LightblueClient - update job status
        DataUpdateRequest updateRequest = new DataUpdateRequest("migrationJob", getJobConfiguration().getMigrationJobEntityVersion());
        updateRequest.where(withValue("_id" + " = " + _id));

        List<Projection> projections = new ArrayList<>();
        projections.add(new FieldProjection("*", true, true));
        updateRequest.setProjections(projections);

        List<Update> updates = new ArrayList<>();

        if (updateEndDate) {
            currentRun.setActualEndDate(new Date());
            updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".actualEndDate", new ObjectRValue(currentRun.getActualEndDate()))));
        }
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".jobStatus", new ObjectRValue(jobStatus.toString()))));
        updateRequest.updates(updates);

        LOGGER.debug("Marking Execution Complete: {}", updateRequest.getBody());
        return callLightblue(updateRequest);
    }

    /**
     *
     * @param jobExecutionPsn if this is the initial save, set to -1
     * @return
     */
    private LightblueResponse saveJobDetails(int jobExecutionPsn) {
        // LightblueClient - update job details
        DataUpdateRequest updateRequest = new DataUpdateRequest("migrationJob", getJobConfiguration().getMigrationJobEntityVersion());
        updateRequest.where(withValue("_id" + " = " + _id));

        List<Projection> projections = new ArrayList<>();
        projections.add(new FieldProjection("*", true, true));
        updateRequest.setProjections(projections);

        List<Update> updates = new ArrayList<>();
        if (jobExecutionPsn < 0) {
            updates.add(new AppendUpdate("jobExecutions", new ObjectRValue(new HashMap<>())));
            jobExecutionPsn = -1;
        }

        // TODO update specific job execution...
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".ownerName", new ObjectRValue(currentRun.getOwnerName()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".hostName", new ObjectRValue(currentRun.getHostName()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".pid", new ObjectRValue(currentRun.getPid()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".actualStartDate", new ObjectRValue(currentRun.getActualStartDate()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".actualEndDate", new ObjectRValue(currentRun.getActualEndDate()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".jobStatus", new ObjectRValue(currentRun.getJobStatus().toString()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".processedDocumentCount", new ObjectRValue(currentRun.getProcessedDocumentCount()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".consistentDocumentCount", new ObjectRValue(currentRun.getConsistentDocumentCount()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".inconsistentDocumentCount", new ObjectRValue(currentRun.getInconsistentDocumentCount()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".overwrittenDocumentCount", new ObjectRValue(currentRun.getOverwrittenDocumentCount()))));

        if (currentRun.getSourceQuery() != null && !currentRun.getSourceQuery().isEmpty()) {
            updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".sourceQuery", new ObjectRValue(currentRun.getSourceQuery()))));
        }
        updateRequest.updates(updates);

        LOGGER.debug("save: {}", updateRequest.getBody());

        return callLightblue(updateRequest);
    }

    protected int overwriteLightblue(List<JsonNode> documentsToOverwrite) {
        if (documentsToOverwrite.size() <= BATCH_SIZE) {
            return doOverwriteLightblue(documentsToOverwrite);
        }

        int totalModified = 0;
        int position = 0;

        while (position < documentsToOverwrite.size()) {
            int limitedPosition = position + BATCH_SIZE;
            if (limitedPosition > documentsToOverwrite.size()) {
                limitedPosition = documentsToOverwrite.size();
            }

            List<JsonNode> subList = documentsToOverwrite.subList(position, limitedPosition);
            totalModified += doOverwriteLightblue(subList);

            position = limitedPosition;
        }

        return totalModified;
    }

    private int doOverwriteLightblue(List<JsonNode> documentsToOverwrite) {
        // LightblueClient - save & overwrite documents
        DataSaveRequest saveRequest = new DataSaveRequest(getJobConfiguration().getDestinationEntityName(), getJobConfiguration().getDestinationEntityVersion());
        saveRequest.create(documentsToOverwrite.toArray());
        List<Projection> projections = new ArrayList<>();
        projections.add(new FieldProjection("*", true, true));
        saveRequest.returns(projections);
        LightblueResponse response = callLightblue(saveRequest);
        overwriteProcessedList.add(new AbstractMap.SimpleEntry<>(saveRequest, response));
        return response.parseModifiedCount();
    }

    protected Map<String, JsonNode> getSourceDocuments() throws SQLException, IOException {
        DataFindRequest sourceRequest = new DataFindRequest(getJobConfiguration().getSourceEntityName(), getJobConfiguration().getSourceEntityVersion());
        List<Query> conditions = new LinkedList<>();
        conditions.add(withValue(getJobConfiguration().getSourceTimestampPath() + " >= " + getStartDate()));
        conditions.add(withValue(getJobConfiguration().getSourceTimestampPath() + " <= " + getEndDate()));
        sourceRequest.where(and(conditions));
        sourceRequest.select(includeFieldRecursively("*"));
        currentRun.setSourceQuery(sourceRequest.getBody());
        return findSourceData(sourceRequest);
    }

    protected Map<String, JsonNode> getDestinationDocuments(Map<String, JsonNode> sourceDocuments) throws IOException {
        Map<String, JsonNode> destinationDocuments = new LinkedHashMap<>();
        if (sourceDocuments == null || sourceDocuments.isEmpty()) {
            LOGGER.debug("Unable to fetch any destination documents as there are no source documents");
            return destinationDocuments;
        }

        if (sourceDocuments.size() <= BATCH_SIZE) {
            return doDestinationDocumentFetch(sourceDocuments);
        }

        List<String> keys = Arrays.asList(sourceDocuments.keySet().toArray(new String[0]));
        int position = 0;
        while (position < keys.size()) {
            int limitedPosition = position + BATCH_SIZE;
            if (limitedPosition > keys.size()) {
                limitedPosition = keys.size();
            }

            List<String> subKeys = keys.subList(position, limitedPosition);
            Map<String, JsonNode> batch = new HashMap<>();
            for (String subKey : subKeys) {
                batch.put(subKey, sourceDocuments.get(subKey));
            }

            Map<String, JsonNode> batchRestuls = doDestinationDocumentFetch(batch);
            destinationDocuments.putAll(batchRestuls);
            position = limitedPosition;
        }

        return destinationDocuments;
    }

    private Map<String, JsonNode> doDestinationDocumentFetch(Map<String, JsonNode> sourceDocuments) throws IOException {
        Map<String, JsonNode> destinationDocuments = new LinkedHashMap<>();
        if (sourceDocuments == null || sourceDocuments.isEmpty()) {
            return destinationDocuments;
        }

        DataFindRequest destinationRequest = new DataFindRequest(getJobConfiguration().getDestinationEntityName(), getJobConfiguration().getDestinationEntityVersion());
        List<Query> requestConditions = new LinkedList<>();
        for (Map.Entry<String, JsonNode> sourceDocument : sourceDocuments.entrySet()) {
            List<Query> docConditions = new LinkedList<>();
            for (String keyField : getJobConfiguration().getDestinationIdentityFields()) {
                ValueQuery docQuery = new ValueQuery(keyField + " = " + sourceDocument.getValue().findValue(keyField).asText());
                docConditions.add(docQuery);
            }
            requestConditions.add(and(docConditions));
        }
        destinationRequest.where(or(requestConditions));
        destinationRequest.select(includeFieldRecursively("*"));
        destinationRequest.sort(new SortCondition(getJobConfiguration().getSourceTimestampPath(), SortDirection.ASC));
        destinationDocuments = findDestinationData(destinationRequest);

        return destinationDocuments;
    }

    protected List<JsonNode> getDocumentsToOverwrite(Map<String, JsonNode> sourceDocuments, Map<String, JsonNode> destinationDocuments) {
        List<JsonNode> documentsToOverwrite = new ArrayList<>();
        for (Map.Entry<String, JsonNode> sourceDocument : sourceDocuments.entrySet()) {
            JsonNode destinationDocument = destinationDocuments.get(sourceDocument.getKey());
            if (destinationDocument == null) {
                // doc never existed in dest, don't log, just overwrite
                documentsToOverwrite.add(sourceDocument.getValue());
            } else {
                List<String> inconsistentPaths = getInconsistentPaths(sourceDocument.getValue(), destinationDocument);
                if (inconsistentPaths.size() > 0) {
                    // log what was inconsistent and add to docs to overwrite
                    List<String> idValues = new ArrayList<>();
                    for (String idField : migrationConfiguration.getDestinationIdentityFields()) {
                        //TODO this assumes keys at root, this might not always be true.  fix it..
                        idValues.add(sourceDocument.getValue().get(idField).asText());
                    }

                    // log as key=value to make parsing easy
                    // fields to log: config name, job id, dest entity name & version, id field names & values, list of inconsistent paths
                    LOGGER.error("configurationName={} destinationEntityName={} destinationEntityVersion={} migrationJobId={} identityFields=\"{}\" identityFieldValues=\"{}\" inconsistentPaths=\"{}\"",
                            migrationConfiguration.getConfigurationName(),
                            migrationConfiguration.getDestinationEntityName(),
                            migrationConfiguration.getDestinationEntityVersion(),
                            this._id,
                            StringUtils.join(migrationConfiguration.getDestinationIdentityFields(), ","),
                            StringUtils.join(idValues, ","),
                            StringUtils.join(inconsistentPaths, ","));

                    documentsToOverwrite.add(sourceDocument.getValue());
                }
            }
        }
        return documentsToOverwrite;
    }

    /**
     *
     * @param sourceDocument
     * @param destinationDocument
     * @return list of inconsistent paths
     */
    protected List<String> getInconsistentPaths(JsonNode sourceDocument, JsonNode destinationDocument) {
        List<String> inconsistentPaths = new ArrayList<>();
        doInconsistentPaths(inconsistentPaths, sourceDocument, destinationDocument, null);
        return inconsistentPaths;
    }

    //Recursive method
    private void doInconsistentPaths(List<String> inconsistentPaths, final JsonNode sourceDocument, final JsonNode destinationDocument, final String path) {
        List<String> excludes = getJobConfiguration().getComparisonExclusionPaths();
        if (excludes != null && excludes.contains(path)) {
            return;
        }

        if (sourceDocument == null && destinationDocument == null) {
            return;
        } else if (sourceDocument == null || destinationDocument == null) {
            inconsistentPaths.add(StringUtils.isEmpty(path) ? "*" : path);
            return;
        }

        // for each field compare to destination
        if (JsonNodeType.ARRAY.equals(sourceDocument.getNodeType())) {
            if (!JsonNodeType.ARRAY.equals(destinationDocument.getNodeType())) {
                inconsistentPaths.add(StringUtils.isEmpty(path) ? "*" : path);
                return;
            }

            ArrayNode sourceArray = (ArrayNode) sourceDocument;
            ArrayNode destinationArray = (ArrayNode) destinationDocument;

            if (sourceArray.size() != destinationArray.size()) {
                inconsistentPaths.add(StringUtils.isEmpty(path) ? "*" : path);
                return;
            }

            // compare array contents
            for (int x = 0; x < sourceArray.size(); x++) {
                doInconsistentPaths(inconsistentPaths, sourceArray.get(x), destinationArray.get(x), path);
            }
        } else if (JsonNodeType.OBJECT.equals(sourceDocument.getNodeType())) {
            if (!JsonNodeType.OBJECT.equals(destinationDocument.getNodeType())) {
                inconsistentPaths.add(StringUtils.isEmpty(path) ? "*" : path);
                return;
            }

            // compare object contents
            Iterator<Entry<String, JsonNode>> itr = sourceDocument.fields();

            while (itr.hasNext()) {
                Entry<String, JsonNode> entry = itr.next();

                doInconsistentPaths(inconsistentPaths, entry.getValue(), destinationDocument.get(entry.getKey()),
                        StringUtils.isEmpty(path) ? entry.getKey() : path + "." + entry.getKey());
            }

        } else if (!sourceDocument.asText().equals(destinationDocument.asText())) {
            inconsistentPaths.add(StringUtils.isEmpty(path) ? "*" : path);
        }
    }

    protected Map<String, JsonNode> findSourceData(LightblueRequest findRequest) throws IOException {
        return getJsonNodeMap(getSourceClient().data(findRequest, JsonNode[].class), getJobConfiguration().getDestinationIdentityFields());
    }

    protected Map<String, JsonNode> findDestinationData(LightblueRequest findRequest) throws IOException {
        return getJsonNodeMap(getDestinationClient().data(findRequest, JsonNode[].class), getJobConfiguration().getDestinationIdentityFields());
    }

    protected LinkedHashMap<String, JsonNode> getJsonNodeMap(JsonNode[] results, List<String> entityKeyFields) {
        LinkedHashMap<String, JsonNode> resultsMap = new LinkedHashMap<>();
        for (JsonNode result : results) {
            StringBuilder resultKey = new StringBuilder();
            for (String keyField : entityKeyFields) {
                resultKey.append(result.findValue(keyField)).append("|||");
            }
            resultsMap.put(resultKey.toString(), result);
        }
        return resultsMap;
    }

    protected LightblueResponse callLightblue(LightblueRequest request) {
        LightblueResponse response = getDestinationClient().data(request);
        if (response.hasError()) {
            throw new RuntimeException("Error returned in response " + response.getText() + " for request " + request.getBody());
        }
        return response;
    }
}
