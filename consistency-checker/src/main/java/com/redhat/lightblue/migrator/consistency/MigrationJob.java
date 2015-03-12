package com.redhat.lightblue.migrator.consistency;

import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.and;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.or;
import static com.redhat.lightblue.client.expression.query.ValueQuery.withValue;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;

import java.io.IOException;
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
import com.fasterxml.jackson.databind.node.ObjectNode;
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

    private List<MigrationJobExecution> getJobExecutions() {
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

        try {
            currentRun = new MigrationJobExecution();
            currentRun.setOwnerName(owner);
            currentRun.setHostName(hostName);
            currentRun.setPid(pid);
            currentRun.setActualStartDate(new Date());
            getJobExecutions().add(currentRun);

            configureClients();

            LightblueResponse response = saveJobDetails(-1);

            Object[] x = shouldProcessJob(response);
            boolean processJob = (Boolean) x[0];
            int jobExecutionPsn = (Integer) x[1];

            if (processJob) {
                Map<String, JsonNode> sourceDocuments = getSourceDocuments();

                Map<String, JsonNode> destinationDocuments = getDestinationDocuments(sourceDocuments);

                List<JsonNode> documentsToOverwrite = getDocumentsToOverwrite(sourceDocuments, destinationDocuments);

                if (!documentsToOverwrite.isEmpty()) {
                    hasInconsistentDocuments = true;
                }

                currentRun.setProcessedDocumentCount(sourceDocuments.size());
                currentRun.setConsistentDocumentCount(sourceDocuments.size() - documentsToOverwrite.size());
                currentRun.setInconsistentDocumentCount(documentsToOverwrite.size());

                if (shouldOverwriteDestinationDocuments() && hasInconsistentDocuments) {
                    currentRun.setOverwrittenDocumentCount(overwriteLightblue(documentsToOverwrite));
                }
                currentRun.setCompletedFlag(true);
                currentRun.setActualEndDate(new Date());
                saveJobDetails(jobExecutionPsn);
            } else {
                // just mark complete and set actual end date
                markExecutionComplete(jobExecutionPsn);
            }

        } catch (RuntimeException e) {
            // would be nice to reference DataType.DATE_FORMAT_STR in core..
            DateFormat dateFormat = ClientConstants.getDateFormat();
            LOGGER.error(String.format("Error while processing job %s with %s for start date %s and end date %s",
                    _id,
                    getJobConfiguration(), (getStartDate() == null ? "null" : dateFormat.format(getStartDate())),
                    (getEndDate() == null ? "null" : dateFormat.format(getEndDate()))), e);
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
    private Object[] shouldProcessJob(LightblueResponse response) {
        boolean processJob = true;
        int jobExecutionPsn = -1;

        try {
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
                if (!execution.isCompletedFlag() && !pid.equals(execution.getPid())) {
                    // check if this is a dead job
                    if (System.currentTimeMillis() - execution.getActualStartDate().getTime() > JOB_EXECUTION_TIMEOUT_MSEC) {
                        // job is dead, mark it complete
                        markExecutionComplete(i);
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
        } catch (LightblueResponseParseException e) {
            throw new RuntimeException("Error parsing lightblue response: " + response.getJson().toString(), e);
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

    private LightblueResponse markExecutionComplete(int jobExecutionPsn) {
        DataUpdateRequest updateRequest = new DataUpdateRequest("migrationJob", getJobConfiguration().getMigrationJobEntityVersion());
        updateRequest.where(withValue("_id" + " = " + _id));

        List<Projection> projections = new ArrayList<>();
        projections.add(new FieldProjection("*", true, true));
        updateRequest.setProjections(projections);

        List<Update> updates = new ArrayList<>();

        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".actualEndDate", new ObjectRValue(currentRun.getActualEndDate()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".completedFlag", new ObjectRValue(currentRun.isCompletedFlag()))));
        updateRequest.updates(updates);

        return callLightblue(updateRequest);
    }

    /**
     *
     * @param jobExecutionPsn if this is the initial save, set to -1
     * @return
     */
    private LightblueResponse saveJobDetails(int jobExecutionPsn) {
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
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".completedFlag", new ObjectRValue(currentRun.isCompletedFlag()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".processedDocumentCount", new ObjectRValue(currentRun.getProcessedDocumentCount()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".consistentDocumentCount", new ObjectRValue(currentRun.getConsistentDocumentCount()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".inconsistentDocumentCount", new ObjectRValue(currentRun.getInconsistentDocumentCount()))));
        updates.add(new SetUpdate(new PathValuePair("jobExecutions." + jobExecutionPsn + ".overwrittenDocumentCount", new ObjectRValue(currentRun.getOverwrittenDocumentCount()))));
        updateRequest.updates(updates);

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
        DataSaveRequest saveRequest = new DataSaveRequest(getJobConfiguration().getDestinationEntityName(), getJobConfiguration().getDestinationEntityVersion());
        saveRequest.create(documentsToOverwrite.toArray());
        List<Projection> projections = new ArrayList<>();
        projections.add(new FieldProjection("*", true, true));
        saveRequest.returns(projections);
        LightblueResponse response = callLightblue(saveRequest);
        return response.parseModifiedCount();
    }

    protected Map<String, JsonNode> getSourceDocuments() {
        Map<String, JsonNode> sourceDocuments = new LinkedHashMap<>();
        try {
            DataFindRequest sourceRequest = new DataFindRequest(getJobConfiguration().getSourceEntityName(), getJobConfiguration().getSourceEntityVersion());
            List<Query> conditions = new LinkedList<>();
            conditions.add(withValue(getJobConfiguration().getSourceTimestampPath() + " >= " + getStartDate()));
            conditions.add(withValue(getJobConfiguration().getSourceTimestampPath() + " <= " + getEndDate()));
            sourceRequest.where(and(conditions));
            sourceRequest.select(includeFieldRecursively("*"));
            sourceDocuments = findSourceData(sourceRequest);
        } catch (IOException e) {
            throw new RuntimeException("Problem getting sourceDocuments", e);
        }
        return sourceDocuments;
    }

    protected Map<String, JsonNode> getDestinationDocuments(Map<String, JsonNode> sourceDocuments) {
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

    private Map<String, JsonNode> doDestinationDocumentFetch(Map<String, JsonNode> sourceDocuments) {
        Map<String, JsonNode> destinationDocuments = new LinkedHashMap<>();
        if (sourceDocuments == null || sourceDocuments.isEmpty()) {
            return destinationDocuments;
        }

        try {
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
        } catch (IOException e) {
            throw new RuntimeException("Problem getting destinationDocuments", e);
        }
        return destinationDocuments;
    }

    protected List<JsonNode> getDocumentsToOverwrite(Map<String, JsonNode> sourceDocuments, Map<String, JsonNode> destinationDocuments) {
        List<JsonNode> documentsToOverwrite = new ArrayList<>();
        for (Map.Entry<String, JsonNode> sourceDocument : sourceDocuments.entrySet()) {
            JsonNode destinationDocument = destinationDocuments.get(sourceDocument.getKey());
            if (destinationDocument == null) {
                documentsToOverwrite.add(sourceDocument.getValue());
            } else if (!documentsConsistent(sourceDocument.getValue(), destinationDocument)) {
                documentsToOverwrite.add(sourceDocument.getValue());
            }
        }
        return documentsToOverwrite;
    }

    protected boolean documentsConsistent(JsonNode sourceDocument, JsonNode destinationDocument) {
        return doDocumentsConsistent(sourceDocument, destinationDocument, null);
    }

    //Recursive method
    private boolean doDocumentsConsistent(final JsonNode sourceDocument, final JsonNode destinationDocument, final String path) {
        List<String> excludes = getJobConfiguration().getComparisonExclusionPaths();
        if (excludes != null && excludes.contains(path)) {
            return true;
        }

        if (sourceDocument == null && destinationDocument == null) {
            return true;
        } else if (sourceDocument == null || destinationDocument == null) {
            return false;
        }

        if (JsonNodeType.ARRAY.equals(sourceDocument.getNodeType())) {
            if (!JsonNodeType.ARRAY.equals(destinationDocument.getNodeType())) {
                return false;
            }

            ArrayNode sourceArray = (ArrayNode) sourceDocument;
            ArrayNode destinationArray = (ArrayNode) destinationDocument;

            if (sourceArray.size() != destinationArray.size()) {
                return false;
            }

            //assumed positions in the array should be the same, else inconsistent.
            for (int x = 0; x < sourceArray.size(); x++) {
                if (!doDocumentsConsistent(sourceArray.get(x), destinationArray.get(x), path)) {
                    return false;
                }
            }
        } else if (JsonNodeType.OBJECT.equals(sourceDocument.getNodeType())) {
            if (!JsonNodeType.OBJECT.equals(destinationDocument.getNodeType())) {
                return false;
            }

            ObjectNode sourceObjNode = (ObjectNode) sourceDocument;
            ObjectNode destObjNode = (ObjectNode) destinationDocument;

            //TODO: This check can be enforced after auto-generated fields are excluded from lightblue queries.
            /*
             if(sourceObjNode.size() != destObjNode.size()){
             return false;
             }
             */
            Iterator<Entry<String, JsonNode>> nodeIterator = sourceObjNode.fields();

            while (nodeIterator.hasNext()) {
                Entry<String, JsonNode> sourceEntry = nodeIterator.next();

                JsonNode sourceNode = sourceEntry.getValue();
                JsonNode destinationNode = destObjNode.get(sourceEntry.getKey());

                String childPath = StringUtils.isEmpty(path) ? sourceEntry.getKey() : path + "." + sourceEntry.getKey();

                if (!doDocumentsConsistent(sourceNode, destinationNode, childPath)) {
                    return false;
                }
            }
        } else {
            return sourceDocument.equals(destinationDocument);
        }

        return true;
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
