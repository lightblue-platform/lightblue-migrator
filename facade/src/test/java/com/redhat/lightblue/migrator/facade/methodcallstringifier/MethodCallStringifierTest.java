package com.redhat.lightblue.migrator.facade.methodcallstringifier;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.redhat.lightblue.migrator.facade.methodcallstringifier.LazyMethodCallStringifier;
import com.redhat.lightblue.migrator.facade.model.Country;
import com.redhat.lightblue.migrator.facade.proxy.FacadeProxyFactory.Secret;

public class MethodCallStringifierTest {

    interface Foo {
        public void foo(int x, String y);

        public void foo(Country[] c, int x, String y);

        public void foo(long[] l, int x, String y);

        public void foo(List<Country> c, int x, String y);

        public void foo2(List<Long> l, int x, String y);

        public void foo(String login, @Secret String password);

        public void foo();
    }

    private Method getMethod(String name, Class<?>... types) {
        try {
            return Foo.class.getMethod(name, types);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSimple() {
        Assert.assertEquals("foo(12, string)",
                LazyMethodCallStringifier.stringifyMethodCall(getMethod("foo", int.class, String.class), new Object[]{12, "string"}));
    }

    @Test
    public void testListOfObjects() {
        List<Country> l = new ArrayList<>();
        l.add(new Country(1l, "PL"));
        l.add(new Country(2l, "CA"));

        Assert.assertEquals("foo([PL id=1, CA id=2], 12, string)",
                LazyMethodCallStringifier.stringifyMethodCall(getMethod("foo", List.class, int.class, String.class), new Object[]{l, 12, "string"}));
    }

    @Test
    public void testListOfPrimitives() {
        List<Long> l = new ArrayList<>();
        l.add(1l);
        l.add(2l);

        Assert.assertEquals("foo2([1, 2], 12, string)",
                LazyMethodCallStringifier.stringifyMethodCall(getMethod("foo2", List.class, int.class, String.class), new Object[]{l, 12, "string"}));
    }

    @Test
    public void testArrayOfPrimitives() {
        Long[] arr = new Long[]{1l, 2l};

        Assert.assertEquals("foo([1, 2], 12, string)",
                LazyMethodCallStringifier.stringifyMethodCall(getMethod("foo", long[].class, int.class, String.class), new Object[]{arr, 12, "string"}));
    }

    @Test
    public void testArrayOfObjects() {
        Country[] arr = new Country[]{new Country(1l, "PL"), new Country(2l, "CA")};

        Assert.assertEquals("foo([PL id=1, CA id=2], 12, string)",
                LazyMethodCallStringifier.stringifyMethodCall(getMethod("foo", Country[].class, int.class, String.class), new Object[]{arr, 12, "string"}));
    }

    @Test
    public void testSecret() {
        Assert.assertEquals("foo(login, ****)",
                LazyMethodCallStringifier.stringifyMethodCall(getMethod("foo", String.class, String.class), new Object[]{"login", "password"}));
    }

    @Test
    public void testNoArguments() {
        Assert.assertEquals("foo()",
                LazyMethodCallStringifier.stringifyMethodCall(getMethod("foo"), new Object[]{}));

        Assert.assertEquals("foo()",
                LazyMethodCallStringifier.stringifyMethodCall(getMethod("foo"), null));
    }

}
