package jiff;

import org.junit.Test;
import org.junit.Assert;

public class AbstractFieldFilterTest {

    @Test
    public void basicEq() {
        Assert.assertTrue(AbstractFieldFilter.matches("abc.def.1.e","abc.def.1.e"));
    }

    @Test
    public void basicDiff() {
        Assert.assertFalse(AbstractFieldFilter.matches("abc.def.1.e","abc.def.2.e"));
    }

    @Test
    public void lengthDiff() {
        Assert.assertFalse(AbstractFieldFilter.matches("abc.def.1.e","abc."));
    }
    
    @Test
    public void lengthDiff2() {
        Assert.assertFalse(AbstractFieldFilter.matches("abc.def.1.e","abc.def.1.e."));
    }

    @Test
    public void basicMatch() {
        Assert.assertTrue(AbstractFieldFilter.matches("abc.def.*.e","abc.def.1.e"));
        Assert.assertTrue(AbstractFieldFilter.matches("abc.def.*.e","abc.def.12.e"));
        Assert.assertTrue(AbstractFieldFilter.matches("abc.def.*.e","abc.def.123.e"));
    }

    @Test
    public void basicMatch2() {
        Assert.assertTrue(AbstractFieldFilter.matches("abc.def.*.*","abc.def.1.1"));
        Assert.assertTrue(AbstractFieldFilter.matches("abc.def.*.*","abc.def.12.12"));
        Assert.assertTrue(AbstractFieldFilter.matches("abc.def.*.*","abc.def.123.123"));
    }

    @Test
    public void basicMatch3() {
        Assert.assertTrue(AbstractFieldFilter.matches("abc.def.*","abc.def.1"));
        Assert.assertTrue(AbstractFieldFilter.matches("abc.def.*","abc.def.12"));
        Assert.assertTrue(AbstractFieldFilter.matches("abc.def.*","abc.def.123"));
    }

}
