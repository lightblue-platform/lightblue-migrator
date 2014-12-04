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
    public void testReadConsistencyEntityFeature() {
        togglzRule.enable(LightblueMigrationFeatures.READ_CONSISTENCY_ENTITY);
        Assert.assertTrue(LightblueMigrationFeatures.READ_CONSISTENCY_ENTITY.isActive());
    }

    @Test
    public void testWriteConsistencyEntityFeature() {
        togglzRule.enable(LightblueMigrationFeatures.WRITE_CONSISTENCY_ENTITY);
        Assert.assertTrue(LightblueMigrationFeatures.WRITE_CONSISTENCY_ENTITY.isActive());
    }

}
