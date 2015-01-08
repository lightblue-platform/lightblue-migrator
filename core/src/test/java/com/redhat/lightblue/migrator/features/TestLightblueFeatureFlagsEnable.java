package com.redhat.lightblue.migrator.features;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.togglz.junit.TogglzRule;

public class TestLightblueFeatureFlagsEnable {

    @Rule
    public TogglzRule togglzRule = TogglzRule.allDisabled(LightblueMigrationFeatures.class);

    @Test
    public void testReadSourceEntityFeature() {
        togglzRule.enable(LightblueMigrationFeatures.READ_SOURCE_ENTITY);
        Assert.assertTrue(LightblueMigrationFeatures.READ_SOURCE_ENTITY.isActive());
    }

    @Test
    public void testWriteSourceEntityFeature() {
        togglzRule.enable(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY);
        Assert.assertTrue(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY.isActive());
    }

    @Test
    public void testReadDestinationEntityFeature() {
        togglzRule.enable(LightblueMigrationFeatures.READ_DESTINATION_ENTITY);
        Assert.assertTrue(LightblueMigrationFeatures.READ_DESTINATION_ENTITY.isActive());
    }

    @Test
    public void testWriteDestinationEntityFeature() {
        togglzRule.enable(LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY);
        Assert.assertTrue(LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY.isActive());
    }

    @Test
    public void testCheckReadConsistencyFeature() {
        togglzRule.enable(LightblueMigrationFeatures.CHECK_READ_CONSISTENCY);
        Assert.assertTrue(LightblueMigrationFeatures.CHECK_READ_CONSISTENCY.isActive());
    }

    @Test
    public void testCheckWriteConsistencyFeature() {
        togglzRule.enable(LightblueMigrationFeatures.CHECK_WRITE_CONSISTENCY);
        Assert.assertTrue(LightblueMigrationFeatures.CHECK_WRITE_CONSISTENCY.isActive());
    }

}
