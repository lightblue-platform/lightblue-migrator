package com.redhat.lightblue.migrator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueException;
import com.redhat.lightblue.client.Projection;
import com.redhat.lightblue.client.Query;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataSaveRequest;
import com.redhat.lightblue.client.response.LightblueResponse;
import com.redhat.lightblue.client.response.LightblueResponseException;
import com.redhat.lightblue.client.util.JSON;

public class DefaultMigrator extends Migrator {

    private static final Logger LOGGER=LoggerFactory.getLogger(DefaultMigrator.class);

    protected static final int BATCH_SIZE = 64;

    private LightblueClient sourceCli;
    private LightblueClient destCli;

    public DefaultMigrator(ThreadGroup grp) {
        super(grp);
    }

    private LightblueClient getSourceCli() {
        try {
            if(sourceCli==null) {
                sourceCli=getLightblueClient(getMigrationConfiguration().getSourceConfigPath());
            }
            return sourceCli;
        } catch (Exception e) {
            LOGGER.error("Cannot get source cli:{}",e);
            throw new RuntimeException("Cannot get source client:"+e);
        }
    }

    private LightblueClient getDestCli() {
        try {
            if(destCli==null) {
                destCli=getLightblueClient(getMigrationConfiguration().getDestinationConfigPath());
            }
            return destCli;
        } catch (Exception e) {
            LOGGER.error("Cannot get dest cli:{}",e);
            throw new RuntimeException("Cannot get dest client:"+e);
        }
    }

    @Override
    public List<JsonNode> getSourceDocuments() {
        LOGGER.debug("Retrieving source docs");
        try {
            DataFindRequest sourceRequest = new DataFindRequest(getMigrationConfiguration().getSourceEntityName(),
                                                                getMigrationConfiguration().getSourceEntityVersion());
            sourceRequest.where(Query.query((ContainerNode)JSON.toJsonNode(getMigrationJob().getQuery())));
            sourceRequest.select(Projection.includeFieldRecursively("*"), Projection.excludeField("objectType"));
            LOGGER.debug("Source docs retrieval req: {}",sourceRequest.getBody());
            JsonNode[] results=getSourceCli().data(sourceRequest,JsonNode[].class);
            LOGGER.debug("There are {} source docs",results.length);
            return Arrays.asList(results);
        } catch (Exception e) {
            LOGGER.error("Error while retrieving source documents:{}",e);
            throw new RuntimeException("Cannot retrieve source documents:"+e);
        }
    }

    @Override
    public List<JsonNode> getDestinationDocuments(Collection<Identity> ids) {
        try {
            List<JsonNode> destinationDocuments = new ArrayList<>();
            if (ids == null || ids.isEmpty()) {
                LOGGER.debug("Unable to fetch any destination documents as there are no source documents");
                return destinationDocuments;
            }

            List<Identity> batch=new ArrayList<>();
            for(Identity id:ids) {
                batch.add(id);
                if(batch.size()>=BATCH_SIZE) {
                    doDestinationDocumentFetch(batch,destinationDocuments);
                    batch.clear();
                }
            }

            if(!batch.isEmpty()) {
                doDestinationDocumentFetch(batch,destinationDocuments);
            }
            return destinationDocuments;
        } catch (Exception e) {
            LOGGER.error("Error while retrieving destination documents:{}",e);
            throw new RuntimeException("Cannot retrieve destination documents:"+e);
        }
    }

    private void  doDestinationDocumentFetch(List<Identity> ids,List<JsonNode> dest)
        throws Exception {
        if(ids!=null&&!ids.isEmpty()) {
            DataFindRequest destinationRequest = new DataFindRequest(getMigrationConfiguration().getDestinationEntityName(),
                                                                     getMigrationConfiguration().getDestinationEntityVersion());
            List<Query> requestConditions = new ArrayList<>();
            for (Identity id:ids) {
                List<Query> docConditions = new ArrayList<>();
                int i=0;
                for (String keyField : getMigrationConfiguration().getDestinationIdentityFields()) {
                    Object v=id.get(i);
                    Query docQuery = Query.withValue(keyField,Query.eq,v==null?null:v.toString());
                    docConditions.add(docQuery);
                    i++;
                }
                requestConditions.add(Query.and(docConditions));
            }
            destinationRequest.where(Query.or(requestConditions));
            destinationRequest.select(Projection.includeFieldRecursively("*"), Projection.excludeField("objectType"));
            LOGGER.debug("Fetching destination docs {}",destinationRequest.getBody());
            JsonNode[] nodes=getDestCli().data(destinationRequest, JsonNode[].class);

            if(nodes!=null) {
                LOGGER.debug("There are {} destination docs",nodes.length);
                for(JsonNode node:nodes) {
                    dest.add(node);
                }
            }
        }
    }

    @Override
    public List<LightblueResponse> save(List<JsonNode> docs) throws LightblueException {
        List<LightblueResponse> responses = new ArrayList<>();
        StringBuilder errorMessage = new StringBuilder();

        List<JsonNode> batch=new ArrayList<>();
        for(JsonNode doc:docs) {
            batch.add(doc);
            if(batch.size()>=BATCH_SIZE) {
                try {
                    responses.add(saveBatch(batch));
                } catch (LightblueResponseException ex) {
                    errorMessage.append(ex.getLightblueResponse().getText());
                }
                batch.clear();
            }
        }
        if(!batch.isEmpty()) {
            try {
                responses.add(saveBatch(batch));
                controller.ping();
            } catch (LightblueResponseException ex) {
                errorMessage.append(ex.getLightblueResponse().getText());
            }
        }

        if (errorMessage.length() > 0) {
            throw new LightblueException("Failed saving docs: " + errorMessage);
        }

        return responses;
    }

    @Override
    public String createRangeQuery(Date startDate,Date endDate) {
        StringTokenizer tkz=new StringTokenizer(getMigrationConfiguration().getTimestampFieldName(),", ");
        List<Query> ql=new ArrayList<>();
        while(tkz.hasMoreTokens()) {
            String tok=tkz.nextToken();
            ql.add(Query.and(Query.withValue(tok,Query.gte,startDate),
                             Query.withValue(tok,Query.lt,endDate)));
        }
        if(ql.size()==1) {
            return ql.get(0).toString();
        } else {
            return Query.or(ql).toString();
        }
    }

    private LightblueResponse saveBatch(List<JsonNode> documentsToOverwrite) throws LightblueResponseException {
        // LightblueClient - save & overwrite documents
        DataSaveRequest saveRequest = new DataSaveRequest(getMigrationConfiguration().getDestinationEntityName(),
                                                          getMigrationConfiguration().getDestinationEntityVersion());
        saveRequest.setUpsert(true);
        saveRequest.create(documentsToOverwrite.toArray());
        saveRequest.returns(Projection.includeField("*"));
        LightblueResponse response;
        try {
            response=getDestCli().data(saveRequest);
        } catch (LightblueException ex) {
            // bad things happened, bail!
            throw new RuntimeException(ex);
        }
        return response;
    }

}

