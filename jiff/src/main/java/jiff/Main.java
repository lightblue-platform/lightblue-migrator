package jiff;

import java.util.List;

import java.io.File;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Main {

    public static void main(String[] args) throws Exception {
        JsonDiff diff=new JsonDiff();
        String file1=null;
        String file2=null;
        for(String arg:args) {
            if("-a0".equals(arg))
                diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);
            else if("-a1".equals(arg))
                diff.setOption(JsonDiff.Option.ARRAY_ORDER_SIGNIFICANT);
            else if("-p0".equals(arg))
                diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
            else if("-p1".equals(arg))
                diff.setOption(JsonDiff.Option.RETURN_PARENT_DIFFS);
            else if(file1==null)
                file1=arg;
            else if(file2==null)
                file2=arg;
            else
                throw new RuntimeException("Invalid option:"+arg);
        }
        if(file1!=null&&file2!=null) {
            ObjectMapper mapper=new ObjectMapper();
            JsonNode node1=mapper.readTree(new File(file1));
            JsonNode node2=mapper.readTree(new File(file2));
            List<JsonDelta> list=diff.computeDiff(node1,node2);
            for(JsonDelta x:list)
                System.out.println(x);
        } else
            printHelp();
    }

    private static void printHelp() {
        System.out.println("jiff [options] file1 file2\n"+
                           "\n"+
                           "Options:\n"+
                           "  -a1 : Array ordering significant\n"+
                           "  -a0 : Array ordering not significant (default)\n"+
                           "  -p1 : Return deltas for parents\n"+
                           "  -p0 : Return deltas for leaves only (default)");
    }
}

