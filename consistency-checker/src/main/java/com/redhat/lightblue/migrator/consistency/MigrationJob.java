package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.and;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.or;
import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueClientConfiguration;
import com.redhat.lightblue.client.PropertiesLightblueClientConfiguration;
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
import com.redhat.lightblue.client.request.AbstractLightblueDataRequest;
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataSaveRequest;
import com.redhat.lightblue.client.request.data.DataUpdateRequest;
import com.redhat.lightblue.client.response.LightblueResponse;
import com.redhat.lightblue.client.response.LightblueResponseParseException;
import com.redhat.lightblue.client.util.ClientConstants;

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
    public static final String STATUS_COMPLETE = "COMPLETE";
    public static final String STATUS_PARTIAL = "PARTIAL";
    public static final String STATUS_ASYNC = "ASYNC";
    public static final String STATUS_ERROR = "ERROR";

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
            getJobExecutions().add(currentRun);

            LightblueResponse response = saveJobDetails(-1);
            LOGGER.debug("Start Save Response: {}", response.getText());

            Object[] x = shouldProcessJob(pid, response.parseProcessed(MigrationJob[].class));
            boolean processJob = (Boolean) x[0];
            jobExecutionPsn = (Integer) x[1];

            if (processJob) {
                currentRun.setJobStatus(JobStatus.RUNNING);
                LightblueResponse responseMarkExecutionStatus = markExecutionStatusAndEndDate(jobExecutionPsn, currentRun.getJobStatus(), false);
                LOGGER.debug("Updated Status Response for status {}: {}", currentRun.getJobStatus(), responseMarkExecutionStatus.getText());

                Map<String, JsonNode> sourceDocuments = getSourceDocuments();
                Map<String, JsonNode> destinationDocuments = getDestinationDocuments(sourceDocuments);

                List<JsonNode> documentsToOverwrite = getDocumentsToOverwrite(sourceDocuments, destinationDocuments);
                if (!documentsToOverwrite.isEmpty()) {
                    hasInconsistentDocuments = true;
                }

                currentRun.setProcessedDocumentCount(sourceDocuments.size());
                currentRun.setConsistentDocumentCount(sourceDocuments.size() - documentsToOverwrite.size());
                currentRun.setInconsistentDocumentCount(documentsToOverwrite.size());

                // Check the status of the overwritten documents
                boolean allComplete = true;
                boolean anyPartial = false;
                boolean anyAsync = false;

                if (shouldOverwriteDestinationDocuments() && hasInconsistentDocuments) {
                    List<LightblueResponse> responses = overwriteLightblue(documentsToOverwrite);
                    int totalOverwrittenDocumentCount = 0;

                    for (LightblueResponse r : responses) {
                        totalOverwrittenDocumentCount += r.parseModifiedCount();
                        JsonNode jsonNode = r.getJson();
                        JsonNode jsStatus = jsonNode.findValue("status");
                        if (jsStatus == null || jsStatus instanceof NullNode || jsStatus instanceof MissingNode) {
                            // It should not reach here as is expected lightblue to throw an exception
                            LOGGER.error("In sourceDocuments: Found 'error' in response's status in one of the documents!");
                            throw new RuntimeException("Found 'error' in response's status!");
                        }
                        String status = jsStatus.asText(); // TODO lightblue client should have a better way to handle this
                        if (!STATUS_COMPLETE.equalsIgnoreCase(status)) {
                            allComplete = false;
                            if (STATUS_PARTIAL.equalsIgnoreCase(status)) {
                                anyPartial = true;
                            } else if (STATUS_ASYNC.equalsIgnoreCase(status)) {
                                anyAsync = true;
                            } else if (STATUS_ERROR.equalsIgnoreCase(status)) {
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
                    currentRun.setOverwrittenDocumentCount(totalOverwrittenDocumentCount);
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
                // mark aborted
                currentRun.setJobStatus(JobStatus.ABORTED_DUPLICATE);
                currentRun.setActualEndDate(new Date());
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
            } catch (IOException ex) {
                LOGGER.error("Couldn't update failed job's status", ex);
            }
        }

        LOGGER.debug("MigrationJob completed");
    }

    /**
     * Return array with two elements indicating if the job should process and
     * what the index of the jobExecution is.
     *
     * @param pid the pid of the current job execution
     * @param jobs the jobs that came back from lightblue
     * @return object array where index 0 is a Boolean indicating if the job
     * should be processed, index 1 is an Integer indicating the position in the
     * jobExecutions array for this job execution attempt
     */
    protected static Object[] shouldProcessJob(String pid, MigrationJob[] jobs) throws LightblueResponseParseException {
        boolean processJob = true;
        int jobExecutionPsn = -1;

        // should only be one job returned, verify
        if (jobs.length > 1) {
            throw new RuntimeException("Error parsing lightblue response: more than one job returned: " + jobs.length);
        }

        // first incomplete job execution must be for this execution else we don't process it
        int i = 0;
        for (MigrationJobExecution execution : jobs[0].getJobExecutions()) {
            // if we find an execution that is not our pid but is active
            // in the array before ours, we do not get to process
            if (jobExecutionPsn < 0 && !execution.getJobStatus().isCompleted() && !pid.equals(execution.getPid())) {
                // we shouldn't be processing this particular job.  abort.
                processJob = false;
                break;
            }

            // find last job execution for our pid
            if (pid.equals(execution.getPid())) {
                jobExecutionPsn = i;
            } else if (jobExecutionPsn >= 0) {
                // we have found the last execution matching our pid.
                // this should be the "current" execution.
                break;
            }

            // increment position in array
            i++;
        }

        return new Object[]{processJob, jobExecutionPsn};
    }

    private void configureClients() throws IOException {
        if (null == sourceClient || null == destinationClient) {
            synchronized (this) {
                if (null == sourceClient || null == destinationClient) {
                    LightblueHttpClient source;
                    LightblueHttpClient destination;
                    if (getSourceConfigPath() == null && getDestinationConfigPath() == null) {
                        source = new LightblueHttpClient();
                        destination = new LightblueHttpClient();
                    } else {
                        // load from config files
                        try (InputStream is = Thread.currentThread()
                                .getContextClassLoader().getResourceAsStream(getSourceConfigPath())) {
                            LightblueClientConfiguration config = PropertiesLightblueClientConfiguration.fromInputStream(is);
                            source = new LightblueHttpClient(config);
                        }

                        try (InputStream is = Thread.currentThread()
                                .getContextClassLoader().getResourceAsStream(getDestinationConfigPath())) {
                            LightblueClientConfiguration config = PropertiesLightblueClientConfiguration.fromInputStream(is);
                            destination = new LightblueHttpClient(config);
                        }
                    }
                    sourceClient = new LightblueHystrixClient(source, "migrator", "sourceClient");
                    destinationClient = new LightblueHystrixClient(destination, "migrator", "destinationClient");

                }
            }
        }
    }

    /**
     * NOTE this does not change the objects in memory!
     *
     * @param jobExecutionPsn
     * @param jobStatus
     * @param updateEndDate
     * @return
     */
    protected LightblueResponse markExecutionStatusAndEndDate(int jobExecutionPsn, JobStatus jobStatus, boolean updateEndDate) throws IOException {
        // LightblueClient - update job status
        DataUpdateRequest updateRequest = new DataUpdateRequest("migrationJob", getJobConfiguration().getMigrationJobEntityVersion());
        updateRequest.where(withValue("_id" + " = " + _id));

        List<Projection> projections = new ArrayList<>();
        projections.add(new FieldProjection("*", true, true));
        updateRequest.setProjections(projections);

        List<Update> updates = new ArrayList<>();

        if (updateEndDate) {
            updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".actualEndDate", new ObjectRValue(new Date()))));
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
    private LightblueResponse saveJobDetails(int jobExecutionPsn) throws IOException {
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

    protected List<LightblueResponse> overwriteLightblue(List<JsonNode> documentsToOverwrite) throws IOException {
        List<LightblueResponse> responses = new ArrayList<>();
        if (documentsToOverwrite.size() <= BATCH_SIZE) {
            responses.add(doOverwriteLightblue(documentsToOverwrite));
            return responses;
        }

        int totalModified = 0;
        int position = 0;

        while (position < documentsToOverwrite.size()) {
            int limitedPosition = position + BATCH_SIZE;
            if (limitedPosition > documentsToOverwrite.size()) {
                limitedPosition = documentsToOverwrite.size();
            }

            List<JsonNode> subList = documentsToOverwrite.subList(position, limitedPosition);
            responses.add(doOverwriteLightblue(subList));

            position = limitedPosition;
        }

        return responses;
    }

    private LightblueResponse doOverwriteLightblue(List<JsonNode> documentsToOverwrite) throws IOException {
        // LightblueClient - save & overwrite documents
        DataSaveRequest saveRequest = new DataSaveRequest(getJobConfiguration().getDestinationEntityName(), getJobConfiguration().getDestinationEntityVersion());
        saveRequest.create(documentsToOverwrite.toArray());
        List<Projection> projections = new ArrayList<>();
        projections.add(new FieldProjection("*", false, true));
        saveRequest.returns(projections);
        return callLightblue(saveRequest);
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

    protected Map<String, JsonNode> findSourceData(AbstractLightblueDataRequest findRequest) throws IOException {
        configureClients();
        return getJsonNodeMap(getSourceClient().data(findRequest, JsonNode[].class), getJobConfiguration().getDestinationIdentityFields());
    }

    protected Map<String, JsonNode> findDestinationData(AbstractLightblueDataRequest findRequest) throws IOException {
        configureClients();
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

    protected LightblueResponse callLightblue(AbstractLightblueDataRequest request) throws IOException {
        configureClients();
        LightblueResponse response = getDestinationClient().data(request);
        if (response.hasError()) {
            throw new RuntimeException("Error returned in response " + response.getText() + " for request " + request.getBody());
        }
        return response;
    }
}
