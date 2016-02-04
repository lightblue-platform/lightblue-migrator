package com.redhat.lightblue.migrator.features;

import java.io.IOException;

import javax.naming.NamingException;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test reading state provider configurations from a property file.
 *
 * @author mpatercz
 *
 */
public class LightblueMigrationStateRepositoryProviderTest {

    @Test
    public void testInitFromPropertyFile() throws IOException, NamingException {

        LightblueMigrationStateRepositoryProvider provider = new LightblueMigrationStateRepositoryProvider("features-config.properties");

        Assert.assertEquals("java:/TestDS", provider.getDataSourceJndi());
        Assert.assertEquals("Test", provider.getTableName());
        Assert.assertEquals(-1, provider.getCacheSeconds());
        Assert.assertEquals(true, provider.isNoCommit());
    }

}
