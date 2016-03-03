package jiff;

import org.junit.Test;
import org.junit.Assert;

public class AbstractFieldFilterTest {

    private boolean matches(String s1,String s2) {
        return AbstractFieldFilter.matches(AbstractFieldFilter.parse(s1),
                                           AbstractFieldFilter.parse(s2));
    }
    
    @Test
    public void basicEq() {
        Assert.assertTrue(matches("abc.def.1.e","abc.def.1.e"));
    }

    @Test
    public void basicDiff() {
        Assert.assertFalse(matches("abc.def.1.e","abc.def.2.e"));
    }


    @Test
    public void basicMatch() {
        Assert.assertTrue(matches("abc.def.*.e","abc.def.1.e"));
        Assert.assertTrue(matches("abc.def.*.e","abc.def.12.e"));
        Assert.assertTrue(matches("abc.def.*.e","abc.def.123.e"));
    }

    @Test
    public void basicMatch2() {
        Assert.assertTrue(matches("abc.def.*.*","abc.def.1.1"));
        Assert.assertTrue(matches("abc.def.*.*","abc.def.12.12"));
        Assert.assertTrue(matches("abc.def.*.*","abc.def.123.123"));
    }

    @Test
    public void basicMatch3() {
        Assert.assertTrue(matches("abc.def.*","abc.def.1"));
        Assert.assertTrue(matches("abc.def.*","abc.def.12"));
        Assert.assertTrue(matches("abc.def.*","abc.def.123"));
    }

}
