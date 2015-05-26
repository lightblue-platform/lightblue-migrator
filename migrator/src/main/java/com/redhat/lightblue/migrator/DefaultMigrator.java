package com.redhat.lightblue.migrator;

import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import java.io.IOException;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.enums.SortDirection;
import com.redhat.lightblue.client.expression.query.Query;
import com.redhat.lightblue.client.expression.query.ValueQuery;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;
import static com.redhat.lightblue.client.projection.FieldProjection.excludeField;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.and;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.or;

public class DefaultMigrator extends Migrator {

    private static final Logger LOGGER=LoggerFactory.getLogger(DefaultMigrator.class);

    protected static final int BATCH_SIZE = 64;

    @Override
    public String migrate() {
        // Read from source, write to dest
        try {
            LightblueClient sourceCli=getLightblueClient(getMigrationConfiguration().getSourceConfigPath());
            LightblueClient destCli=getLightblueClient(getMigrationConfiguration().getDestinationConfigPath());
            Map<String, JsonNode> sourceDocuments = getSourceDocuments(sourceCli);
            Map<String, JsonNode> destinationDocuments = getDestinationDocuments(destCli,sourceDocuments);

        } catch (Exception e) {
            return e.toString();
        }
        return null;
    }

    protected Map<String, JsonNode> getSourceDocuments(LightblueClient cli)
        throws SQLException, IOException {
        DataFindRequest sourceRequest = new DataFindRequest(getMigrationConfiguration().getSourceEntityName(),
                                                            getMigrationConfiguration().getSourceEntityVersion());
        sourceRequest.where(new Query() {
                public String toJson() {
                    return getMigrationJob().getQuery();
                }
            });
        sourceRequest.select(includeFieldRecursively("*"), excludeField("objectType"));
        return getJsonNodeMap(cli.data(sourceRequest, JsonNode[].class),
                              getMigrationConfiguration().getDestinationIdentityFields());
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
    
    protected Map<String, JsonNode> getDestinationDocuments(LightblueClient destCli,
                                                            Map<String, JsonNode> sourceDocuments)
        throws IOException {
        Map<String, JsonNode> destinationDocuments = new LinkedHashMap<>();
        if (sourceDocuments == null || sourceDocuments.isEmpty()) {
            LOGGER.debug("Unable to fetch any destination documents as there are no source documents");
            return destinationDocuments;
        }
        
        if (sourceDocuments.size() <= BATCH_SIZE) {
            return doDestinationDocumentFetch(destCli,sourceDocuments);
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

            Map<String, JsonNode> batchRestuls = doDestinationDocumentFetch(destCli,batch);
            destinationDocuments.putAll(batchRestuls);
            position = limitedPosition;
        }
        
        return destinationDocuments;
    }

    private Map<String, JsonNode> doDestinationDocumentFetch(LightblueClient destCli,
                                                             Map<String, JsonNode> sourceDocuments)
        throws IOException {
        Map<String, JsonNode> destinationDocuments = new LinkedHashMap<>();
        if (sourceDocuments == null || sourceDocuments.isEmpty()) {
            return destinationDocuments;
        }
        
        DataFindRequest destinationRequest = new DataFindRequest(getMigrationConfiguration().getDestinationEntityName(),
                                                                 getMigrationConfiguration().getDestinationEntityVersion());
        List<Query> requestConditions = new ArrayList<>();
        for (Map.Entry<String, JsonNode> sourceDocument : sourceDocuments.entrySet()) {
            List<Query> docConditions = new ArrayList<>();
            for (String keyField : getMigrationConfiguration().getDestinationIdentityFields()) {
                ValueQuery docQuery = new ValueQuery(keyField + " = " + sourceDocument.getValue().
                                                     findValue(keyField).asText());
                docConditions.add(docQuery);
            }
            requestConditions.add(and(docConditions));
        }
        destinationRequest.where(or(requestConditions));
        destinationRequest.select(includeFieldRecursively("*"), excludeField("objectType"));
        destinationDocuments = getJsonNodeMap(destCli.data(destinationRequest, JsonNode[].class),
                                              getMigrationConfiguration().getDestinationIdentityFields());
        
        return destinationDocuments;
    }

    // protected List<LightblueResponse> overwriteLightblue(List<JsonNode> documentsToOverwrite) throws IOException {
    //     List<LightblueResponse> responses = new ArrayList<>();
    //     if (documentsToOverwrite.size() <= BATCH_SIZE) {
    //         responses.add(doOverwriteLightblue(documentsToOverwrite));
    //         return responses;
    //     }

    //     int totalModified = 0;
    //     int position = 0;

    //     while (position < documentsToOverwrite.size()) {
    //         int limitedPosition = position + BATCH_SIZE;
    //         if (limitedPosition > documentsToOverwrite.size()) {
    //             limitedPosition = documentsToOverwrite.size();
    //         }

    //         List<JsonNode> subList = documentsToOverwrite.subList(position, limitedPosition);
    //         responses.add(doOverwriteLightblue(subList));

    //         position = limitedPosition;
    //     }

    //     return responses;
    // }

    // private LightblueResponse doOverwriteLightblue(List<JsonNode> documentsToOverwrite) throws IOException {
    //     // LightblueClient - save & overwrite documents
    //     DataSaveRequest saveRequest = new DataSaveRequest(getJobConfiguration().getDestinationEntityName(), getJobConfiguration().getDestinationEntityVersion());
    //     saveRequest.create(documentsToOverwrite.toArray());
    //     List<Projection> projections = new ArrayList<>();
    //     projections.add(new FieldProjection("*", false, true));
    //     saveRequest.returns(projections);
    //     return callLightblue(saveRequest);
    // }



    // protected List<JsonNode> getDocumentsToOverwrite(Map<String, JsonNode> sourceDocuments, Map<String, JsonNode> destinationDocuments) {
    //     List<JsonNode> documentsToOverwrite = new ArrayList<>();
    //     for (Map.Entry<String, JsonNode> sourceDocument : sourceDocuments.entrySet()) {
    //         JsonNode destinationDocument = destinationDocuments.get(sourceDocument.getKey());
    //         if (destinationDocument == null) {
    //             // doc never existed in dest, don't log, just overwrite
    //             documentsToOverwrite.add(sourceDocument.getValue());
    //         } else {
    //             List<String> inconsistentPaths = getInconsistentPaths(sourceDocument.getValue(), destinationDocument);
    //             if (inconsistentPaths.size() > 0) {
    //                 // log what was inconsistent and add to docs to overwrite
    //                 List<String> idValues = new ArrayList<>();
    //                 for (String idField : migrationConfiguration.getDestinationIdentityFields()) {
    //                     //TODO this assumes keys at root, this might not always be true.  fix it..
    //                     idValues.add(sourceDocument.getValue().get(idField).asText());
    //                 }

    //                 // log as key=value to make parsing easy
    //                 // fields to log: config name, job id, dest entity name & version, id field names & values, list of inconsistent paths
    //                 LOGGER.error("configurationName={} destinationEntityName={} destinationEntityVersion={} migrationJobId={} identityFields=\"{}\" identityFieldValues=\"{}\" inconsistentPaths=\"{}\"",
    //                         migrationConfiguration.getConfigurationName(),
    //                         migrationConfiguration.getDestinationEntityName(),
    //                         migrationConfiguration.getDestinationEntityVersion(),
    //                         this._id,
    //                         StringUtils.join(migrationConfiguration.getDestinationIdentityFields(), ","),
    //                         StringUtils.join(idValues, ","),
    //                         StringUtils.join(inconsistentPaths, ","));

    //                 documentsToOverwrite.add(sourceDocument.getValue());
    //             }
    //         }
    //     }
    //     return documentsToOverwrite;
    // }

    // /**
    //  *
    //  * @param sourceDocument
    //  * @param destinationDocument
    //  * @return list of inconsistent paths
    //  */
    // protected List<String> getInconsistentPaths(JsonNode sourceDocument, JsonNode destinationDocument) {
    //     List<String> inconsistentPaths = new ArrayList<>();
    //     doInconsistentPaths(inconsistentPaths, sourceDocument, destinationDocument, null);
    //     return inconsistentPaths;
    // }

    // //Recursive method
    // private void doInconsistentPaths(List<String> inconsistentPaths, final JsonNode sourceDocument, final JsonNode destinationDocument, final String path) {
    //     List<String> excludes = getJobConfiguration().getComparisonExclusionPaths();
    //     if (excludes != null && excludes.contains(path)) {
    //         return;
    //     }

    //     if (sourceDocument == null && destinationDocument == null) {
    //         return;
    //     } else if (sourceDocument == null || destinationDocument == null) {
    //         inconsistentPaths.add(StringUtils.isEmpty(path) ? "*" : path);
    //         return;
    //     }

    //     // for each field compare to destination
    //     if (JsonNodeType.ARRAY.equals(sourceDocument.getNodeType())) {
    //         if (!JsonNodeType.ARRAY.equals(destinationDocument.getNodeType())) {
    //             inconsistentPaths.add(StringUtils.isEmpty(path) ? "*" : path);
    //             return;
    //         }

    //         ArrayNode sourceArray = (ArrayNode) sourceDocument;
    //         ArrayNode destinationArray = (ArrayNode) destinationDocument;

    //         if (sourceArray.size() != destinationArray.size()) {
    //             inconsistentPaths.add(StringUtils.isEmpty(path) ? "*" : path);
    //             return;
    //         }

    //         // compare array contents
    //         for (int x = 0; x < sourceArray.size(); x++) {
    //             doInconsistentPaths(inconsistentPaths, sourceArray.get(x), destinationArray.get(x), path);
    //         }
    //     } else if (JsonNodeType.OBJECT.equals(sourceDocument.getNodeType())) {
    //         if (!JsonNodeType.OBJECT.equals(destinationDocument.getNodeType())) {
    //             inconsistentPaths.add(StringUtils.isEmpty(path) ? "*" : path);
    //             return;
    //         }

    //         // compare object contents
    //         Iterator<Entry<String, JsonNode>> itr = sourceDocument.fields();

    //         while (itr.hasNext()) {
    //             Entry<String, JsonNode> entry = itr.next();

    //             doInconsistentPaths(inconsistentPaths, entry.getValue(), destinationDocument.get(entry.getKey()),
    //                     StringUtils.isEmpty(path) ? entry.getKey() : path + "." + entry.getKey());
    //         }

    //     } else if (!sourceDocument.asText().equals(destinationDocument.asText())) {
    //         inconsistentPaths.add(StringUtils.isEmpty(path) ? "*" : path);
    //     }
    // }


}

