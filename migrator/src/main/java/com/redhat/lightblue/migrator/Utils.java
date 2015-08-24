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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;

import org.skyscreamer.jsonassert.JSONCompareResult;
import org.skyscreamer.jsonassert.JSONCompare;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.FieldComparisonFailure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ArrayNode;

import com.redhat.lightblue.client.LightblueClient;
import com.redhat.lightblue.client.LightblueClientConfiguration;
import com.redhat.lightblue.client.PropertiesLightblueClientConfiguration;
import com.redhat.lightblue.client.util.ClientConstants;
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
    public static List<Inconsistency> compareDocs(JsonNode sourceDocument, JsonNode destinationDocument,List<String> exclusionPaths) {
        List<Inconsistency> inconsistencies = new ArrayList<>();
        try {
            JSONCompareResult result = JSONCompare.compareJSON(sourceDocument.toString(),
                                                               destinationDocument.toString(),
                                                               JSONCompareMode.STRICT);
            for(FieldComparisonFailure x:result.getFieldFailures()) {
                String field=toPath(x.getField());
                if(!isExcluded(exclusionPaths,field)) {
                    if(reallyDifferent(x.getExpected(),x.getActual())) {
                        inconsistencies.add(new Inconsistency(field,x.getExpected(),x.getActual()));
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
    private static boolean reallyDifferent(Object source,Object dest) {
        if(source==null)
            if(dest==null)
                return false;
            else
                return true;
        else if(dest==null)
            return true;
        else {
            String s1=source.toString();
            String s2=dest.toString();
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
    

    /**
     * Converts a JSONAssert path string to a lightblue path string
     * That is, [number] is converted to .number
     */
    private static String toPath(String s) {
        int n=s.length();
        StringBuilder dest=new StringBuilder(n);
        for(int i=0;i<n;i++) {
            char c=s.charAt(i);
            if(c=='[')
                dest.append('.');
            else if(c!=']')
                dest.append(c);
        }
        return dest.toString();
    }
}
