package com.redhat.lightblue.migrator.features;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.togglz.junit.TogglzRule;

public class TestLightblueMigrationEnable {

    @Rule
    public TogglzRule togglzRule = TogglzRule.allDisabled(LightblueMigrationFeatures.class);

    @Test
    public void testReadSourceEntityFeature() {
        togglzRule.enable(LightblueMigrationFeatures.READ_SOURCE_ENTITY);
        Assert.assertTrue(LightblueMigration.shouldReadSourceEntity());
    }

    @Test
    public void testWriteSourecEntityFeature() {
        togglzRule.enable(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY);
        Assert.assertTrue(LightblueMigration.shouldWriteSourceEntity());
    }

    @Test
    public void testReadDestinationEntityFeature() {
        togglzRule.enable(LightblueMigrationFeatures.READ_DESTINATION_ENTITY);
        Assert.assertTrue(LightblueMigration.shouldReadDestinationEntity());
    }

    @Test
    public void testWriteDestinationEntityFeature() {
        togglzRule.enable(LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY);
        Assert.assertTrue(LightblueMigration.shouldWriteDestinationEntity());
    }

    @Test
    public void testCheckReadConsistencyFeature() {
        togglzRule.enable(LightblueMigrationFeatures.CHECK_READ_CONSISTENCY);
        Assert.assertTrue(LightblueMigration.shouldCheckReadConsistency());
    }

    @Test
    public void testCheckWriteConsistencyFeature() {
        togglzRule.enable(LightblueMigrationFeatures.CHECK_WRITE_CONSISTENCY);
        Assert.assertTrue(LightblueMigration.shouldCheckWriteConsistency());
    }

}
