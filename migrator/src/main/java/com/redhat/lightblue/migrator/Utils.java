package com.redhat.lightblue.migrator;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Date;

import java.text.DateFormat;

import java.io.InputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.File;
import java.io.StringReader;
import java.io.BufferedReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.redhat.jiff.JsonDiff;
import com.redhat.jiff.JsonDelta;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueClientConfiguration;
import com.redhat.lightblue.client.PropertiesLightblueClientConfiguration;
import com.redhat.lightblue.client.util.ClientConstants;
import com.redhat.lightblue.client.hystrix.LightblueHystrixClient;
import com.redhat.lightblue.client.http.LightblueHttpClient;

public class Utils {

    private static final Logger LOGGER=LoggerFactory.getLogger(Utils.class);

    private Utils() {}

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node1=mapper.readTree(new File(args[0]));
        JsonNode node2=mapper.readTree(new File(args[1]));
        List<Inconsistency> list=compareDocs(node1,node2,new ArrayList<String>());
        System.out.println(list);
    }
    
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
    public static List<Inconsistency> compareDocs(JsonNode sourceDocument, JsonNode destinationDocument,List<String> exclusionPaths) {
        List<Inconsistency> inconsistencies = new ArrayList<>();
        try {
            JsonDiff diff=new JsonDiff();
            diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);
            diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
            List<JsonDelta> list=diff.computeDiff(sourceDocument,destinationDocument);
            System.out.println("Failures:"+list);
            for(JsonDelta x:list) {
                String field=x.getField();
                if(!isExcluded(exclusionPaths,field)) {
                    if(reallyDifferent(x.getNode1(),x.getNode2())) {
                        inconsistencies.add(new Inconsistency(field,x.getNode1(),x.getNode2()));
                    }
                } 
            }
        } catch (Exception e) {
            LOGGER.error("Cannot compare docs:{}",e,e);
        }
        return inconsistencies;
    }

    /**
     * This stupidity is required because data types of the source and
     * dest might be different, but they might have the same
     * value. Like, a number represented as string in one system could
     * be an int in another. That is valid, but JSON comparison fails
     * for that, so we check if the two nodes are *really* different,
     * meaning: for anything but dates, check string equivalence. For
     * dates, normalize by TZ and check.
     */
    private static boolean reallyDifferent(JsonNode source,JsonNode dest) {
        if(source==null || source instanceof NullNode)
            if(dest==null||dest instanceof NullNode)
                return false;
            else
                return true;
        else if(dest==null||dest instanceof NullNode)
            return true;
        else {
            String s1=source.asText();
            String s2=dest.asText();
            if(s1.equals(s2))
                return false;

            // They are different strings
            // Do they look like dates?
            Date d1;
            Date d2;
            DateFormat fmt=ClientConstants.getDateFormat();
            try {
                d1=fmt.parse(s1);
            } catch (Exception e) {
                return true;
            }
            try {
                d2=fmt.parse(s2);
            } catch (Exception e) {
                return true;
            }
            return d1.getTime()!=d2.getTime();
        }
    }

    private static boolean isExcluded(List<String> exclusions,String path) {
        if(!exclusions.contains(path)) {
            for(String x:exclusions)
                if(matches(path,x))
                    return true;
        } else
            return true;
        return false;
    }

    private static boolean matches(String path,String pattern) {
        String[] patternComponents=pattern.split("\\.");
        String[] pathComponents=path.split("\\.");
        if(patternComponents.length<=pathComponents.length) {
            for(int i=0;i<patternComponents.length;i++) {
                if(!(pathComponents[i].equals(patternComponents[i])||
                     "*".equals(patternComponents[i]))) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    
}
