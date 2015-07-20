package com.redhat.lightblue.migrator;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import java.io.InputStream;
import java.io.IOException;
import java.io.FileInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ArrayNode;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueClientConfiguration;
import com.redhat.lightblue.client.PropertiesLightblueClientConfiguration;
import com.redhat.lightblue.client.hystrix.LightblueHystrixClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;

public class Utils {

    private static final Logger LOGGER=LoggerFactory.getLogger(Utils.class);

    private Utils() {}

    public static LightblueClient getLightblueClient(String configPath)
        throws IOException {
        LOGGER.debug("Getting client with config {}",configPath);
        LightblueClient cli;
        if (configPath == null) {
            cli = new LightblueHttpClient();
        } else {
            try (InputStream is = new FileInputStream(configPath)) {
                LightblueClientConfiguration config = PropertiesLightblueClientConfiguration.fromInputStream(is);
                cli = new LightblueHttpClient(config);
            }
        }       
        return new LightblueHystrixClient(cli, "migrator", "cli");
    }

    /**
     * Build an id-doc map from a list of docs
     */
    public static Map<Identity,JsonNode> getDocumentIdMap(List<JsonNode> list,List<String> identityFields) {
        Map<Identity,JsonNode> map=new HashMap<>();
        if(list!=null) {
            LOGGER.debug("Getting doc IDs for {} docs, fields={}",list.size(),identityFields);
            for(JsonNode node:list) {
                Identity id=new Identity(node, identityFields);
                LOGGER.debug("ID={}",id);
                map.put(id,node);
            }
        }
        return map;
    }
    
    /**
     *
     * @param sourceDocument
     * @param destinationDocument
     * @return list of inconsistent paths
     */
    public static List<String> compareDocs(JsonNode sourceDocument, JsonNode destinationDocument,List<String> exclusionPaths) {
        List<String> inconsistentPaths = new ArrayList<>();
        compareDocs(inconsistentPaths, sourceDocument, destinationDocument, null,exclusionPaths);
        return inconsistentPaths;
    }

    //Recursive method
    private static void compareDocs(List<String> inconsistentPaths,
                                    final JsonNode sourceDocument,
                                    final JsonNode destinationDocument,
                                    final String path,
                                    final List<String> excludes) {
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
                compareDocs(inconsistentPaths, sourceArray.get(x), destinationArray.get(x), path,excludes);
            }
        } else if (JsonNodeType.OBJECT.equals(sourceDocument.getNodeType())) {
            if (!JsonNodeType.OBJECT.equals(destinationDocument.getNodeType())) {
                inconsistentPaths.add(StringUtils.isEmpty(path) ? "*" : path);
                return;
            }

            // compare object contents
            Iterator<Map.Entry<String, JsonNode>> itr = sourceDocument.fields();

            while (itr.hasNext()) {
                Map.Entry<String, JsonNode> entry = itr.next();

                compareDocs(inconsistentPaths, entry.getValue(), destinationDocument.get(entry.getKey()),
                            StringUtils.isEmpty(path) ? entry.getKey() : path + "." + entry.getKey(),excludes);
            }

        } else if (!sourceDocument.asText().equals(destinationDocument.asText())) {
            inconsistentPaths.add(StringUtils.isEmpty(path) ? "*" : path);
        }
    }


}
