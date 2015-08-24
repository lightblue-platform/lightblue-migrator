package com.redhat.lightblue.migrator;

import org.skyscreamer.jsonassert.JSONCompare;
import org.skyscreamer.jsonassert.JSONCompareResult;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.FieldComparisonFailure;

import org.junit.Test;

/**
 * This doesn't test anything, it is here only to figure out how JSONCompare works.
 */
public class JsTest {

    @Test
    public void test() throws Exception {
        String s1="{ \"f1\":1, \"obj\":{ \"f2\":2}, \"arr\":[ {\"f4\":3 } ] }";
        String s2="{ \"f1\":2, \"obj\":{ \"f2\":3 }, \"arr\":[ {\"f4\":4 } ] }";
        JSONCompareResult result = JSONCompare.compareJSON(s1, s2,
                                                           JSONCompareMode.STRICT);
        for(FieldComparisonFailure x:result.getFieldFailures())
            System.out.println(x.getField()+" "+x.getExpected()+" "+x.getActual());
    }
}
