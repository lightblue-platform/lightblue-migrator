package com.redhat.jiff;

import java.util.List;

import org.junit.Test;
import org.junit.Assert;

public class JsonDiffTest {
    public static String esc(String s) {
        return s.replaceAll("\'","\"");
    }

    @Test
    public void basicEq() throws Exception {
        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);
        diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
        List<JsonDelta> list=diff.computeDiff(esc("{'a':1,'b':'x','c':[1,2,3]}"),
                                              esc("{'b':'x','a':1,'c':[2,3,1]}"));
        Assert.assertEquals(0,list.size());
    }

    @Test
    public void basicDiff() throws Exception {
        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);
        diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
        List<JsonDelta> list=diff.computeDiff(esc("{'a':1,'b':'x','c':[1,2,3]}"),
                                              esc("{'b':'y','a':1,'c':[2,3,1]}"));
        Assert.assertEquals(1,list.size());
        Assert.assertEquals("b",list.get(0).getField());
    }

    @Test
    public void moreFields() throws Exception {
        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);
        diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
        List<JsonDelta> list=diff.computeDiff(esc("{'a':1,'b':'x','c':[1,2,3]}"),
                                              esc("{'b':'x','a':1,'c':[2,3,1],'d':1}"));
        Assert.assertEquals(1,list.size());
        Assert.assertEquals("d",list.get(0).getField());
    }

    @Test
    public void objectCmp() throws Exception {
        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);
        diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
        List<JsonDelta> list=diff.computeDiff(esc("{'a':1,'b':'x','c':[1,2,3],'d':{'a':1,'b':2}}"),
                                              esc("{'b':'x','a':1,'c':[2,3,1],'d':{'a':1,'b':2}}"));
        Assert.assertEquals(0,list.size());
    }

    @Test
    public void objectCmpArrOrder() throws Exception {
        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_SIGNIFICANT);
        diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
        List<JsonDelta> list=diff.computeDiff(esc("{'a':1,'b':'x','c':[1,2,3],'d':{'a':1,'b':2}}"),
                                              esc("{'b':'x','a':1,'c':[2,3,1],'d':{'a':1,'b':2}}"));
        Assert.assertEquals(3,list.size());
    }

    @Test
    public void arrCmp() throws Exception {
        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);
        diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
        List<JsonDelta> list=diff.computeDiff(esc("{'a':1,'b':'x','c':[1,2,3],'d':[ {'a':1},{'b':2} ] }"),
                                              esc("{'b':'x','a':1,'c':[2,3,1],'d':[ {'b':2}, {'a':1} ] }"));
        Assert.assertEquals(0,list.size());
    }

    @Test
    public void arrCmpOrder() throws Exception {
        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_SIGNIFICANT);
        diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
        List<JsonDelta> list=diff.computeDiff(esc("{'a':1,'b':'x','c':[1,2,3],'d':[ {'a':1},{'b':2} ] }"),
                                              esc("{'b':'x','a':1,'c':[2,3,1],'d':[ {'b':2}, {'a':1} ] }"));
        Assert.assertEquals(7,list.size());
    }

    @Test
    public void arrCmp_moreElems1() throws Exception {
        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);
        diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
        List<JsonDelta> list=diff.computeDiff(esc("{'a':1,'b':'x','c':[1,2,3],'d':[ {'a':1},{'b':2},{'a':1} ] }"),
                                              esc("{'b':'x','a':1,'c':[2,3,1],'d':[ {'b':2}, {'a':1} ] }"));
        Assert.assertEquals(1,list.size());
    }

    @Test
    public void arrCmp_moreElems1wnull() throws Exception {
        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);
        diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
        List<JsonDelta> list=diff.computeDiff(esc("{'a':1,'b':'x','c':[1,2,3],'d':[ {'a':1},{'b':2},{'a':1} ] }"),
                                              esc("{'b':null,'a':1,'c':[2,3,1],'d':[ {'b':2}, {'a':1} ] }"));
        Assert.assertEquals(2,list.size());
    }

    @Test
    public void arrCmp_moreElems2() throws Exception {
        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);
        diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
        List<JsonDelta> list=diff.computeDiff(esc("{'a':1,'b':'x','c':[1,2,3],'d':[ {'a':1},{'b':2} ] }"),
                                              esc("{'b':'x','a':1,'c':[2,3,1],'d':[ {'b':2}, {'a':1},{'a':1} ] }"));
        Assert.assertEquals(1,list.size());
    }

    @Test
    public void arrCmp_moreElems2wnull() throws Exception {
        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);
        diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
        List<JsonDelta> list=diff.computeDiff(esc("{'a':1,'b':null,'c':[1,2,3],'d':[ {'a':1},{'b':2} ] }"),
                                              esc("{'b':'x','a':1,'c':[2,3,1],'d':[ {'b':2}, {'a':1},{'a':1} ] }"));
        Assert.assertEquals(2,list.size());
    }

    @Test
    public void arrCmp_arrnull() throws Exception {
        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);
        diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
        List<JsonDelta> list=diff.computeDiff(esc("{'a':1,'b':'x','c':[1,2,3],'d':null }"),
                                              esc("{'b':'x','a':1,'c':[2,3,1],'d':[ {'b':2}, {'a':1},{'a':1} ] }"));
        Assert.assertEquals(1,list.size());
    }
    
    @Test
    public void arrCmp_2null() throws Exception {
        JsonDiff diff=new JsonDiff();
        diff.setOption(JsonDiff.Option.ARRAY_ORDER_INSIGNIFICANT);
        diff.setOption(JsonDiff.Option.RETURN_LEAVES_ONLY);
        List<JsonDelta> list=diff.computeDiff(esc("{'a':1,'b':'x','c':[1,2,3],'d':null }"),
                                              esc("{'b':'x','a':1,'c':[2,3,1],'d':null }"));
        Assert.assertEquals(0,list.size());
    }
}
