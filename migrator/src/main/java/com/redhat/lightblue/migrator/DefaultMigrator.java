package com.redhat.lightblue.migrator;

import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import java.io.IOException;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.commons.lang.StringUtils;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.request.data.DataFindRequest;
import com.redhat.lightblue.client.request.data.DataSaveRequest;
import com.redhat.lightblue.client.request.SortCondition;
import com.redhat.lightblue.client.response.LightblueResponse;
import com.redhat.lightblue.client.response.LightblueException;
import com.redhat.lightblue.client.enums.SortDirection;
import com.redhat.lightblue.client.enums.ExpressionOperation;
import com.redhat.lightblue.client.expression.query.Query;
import com.redhat.lightblue.client.expression.query.ValueQuery;
import com.redhat.lightblue.client.projection.Projection;
import com.redhat.lightblue.client.projection.FieldProjection;
import static com.redhat.lightblue.client.projection.FieldProjection.includeFieldRecursively;
import static com.redhat.lightblue.client.projection.FieldProjection.excludeField;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.and;
import static com.redhat.lightblue.client.expression.query.NaryLogicalQuery.or;

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
            if(sourceCli==null)
                sourceCli=getLightblueClient(getMigrationConfiguration().getSourceConfigPath());
            return sourceCli;
        } catch (Exception e) {
            LOGGER.error("Cannot get source cli:{}",e);
            throw new RuntimeException("Cannot get source client:"+e);
        }
    }

    private LightblueClient getDestCli() {
        try {
            if(destCli==null)
                destCli=getLightblueClient(getMigrationConfiguration().getDestinationConfigPath());
            return destCli;
        } catch (Exception e) {
            LOGGER.error("Cannot get dest cli:{}",e);
            throw new RuntimeException("Cannot get dest client:"+e);
        }
    }
    
    public List<JsonNode> getSourceDocuments() {
        LOGGER.debug("Retrieving source docs");
        try {
            DataFindRequest sourceRequest = new DataFindRequest(getMigrationConfiguration().getSourceEntityName(),
                                                                getMigrationConfiguration().getSourceEntityVersion());
            sourceRequest.where(new Query() {
                    public String toJson() {
                        return getMigrationJob().getQuery();
                    }
                });
            sourceRequest.select(includeFieldRecursively("*"), excludeField("objectType"));
            LOGGER.debug("Source docs retrieval req: {}",sourceRequest.getBody());
            JsonNode[] results=getSourceCli().data(sourceRequest,JsonNode[].class);
            LOGGER.debug("There are {} source docs",results.length);
            return Arrays.asList(results);
        } catch (Exception e) {
            LOGGER.error("Error while retrieving source documents:{}",e);
            throw new RuntimeException("Cannot retrieve source documents:"+e);
        }
    }
  
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
                    ValueQuery docQuery = new ValueQuery(keyField,ExpressionOperation.EQ,v==null?null:v.toString());
                    docConditions.add(docQuery);
                    i++;
                }
                requestConditions.add(and(docConditions));
            }
            destinationRequest.where(or(requestConditions));
            destinationRequest.select(includeFieldRecursively("*"), excludeField("objectType"));
            LOGGER.debug("Fetching destination docs {}",destinationRequest.getBody());
            JsonNode[] nodes=getDestCli().data(destinationRequest, JsonNode[].class);

            if(nodes!=null) {
                LOGGER.debug("There are {} destination docs",nodes.length);
                for(JsonNode node:nodes)
                    dest.add(node);
            }
        }
    }

    public List<LightblueResponse> save(List<JsonNode> docs) {
        List<LightblueResponse> responses = new ArrayList<>();
        List<JsonNode> batch=new ArrayList<>();
        for(JsonNode doc:docs) {
            batch.add(doc);
            if(batch.size()>=BATCH_SIZE) {
                responses.add(saveBatch(batch));
                batch.clear();
            }
        }
        if(!batch.isEmpty()) {
            responses.add(saveBatch(batch));
        }
        return responses;
    }
                
    private LightblueResponse saveBatch(List<JsonNode> documentsToOverwrite) {        
        // LightblueClient - save & overwrite documents
        DataSaveRequest saveRequest = new DataSaveRequest(getMigrationConfiguration().getDestinationEntityName(),
                                                          getMigrationConfiguration().getDestinationEntityVersion());
        saveRequest.create(documentsToOverwrite.toArray());
        List<Projection> projections = new ArrayList<>();
        projections.add(new FieldProjection("*", false, true));
        saveRequest.returns(projections);
        LightblueResponse response;
        try {
            response=getDestCli().data(saveRequest);
        } catch (LightblueException e) {
            response=e.getLightblueResponse();
        }
        return response;
    }

}

