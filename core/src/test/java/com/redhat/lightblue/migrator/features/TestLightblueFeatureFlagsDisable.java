package com.redhat.lightblue.migrator.features;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.togglz.junit.TogglzRule;

public class TestLightblueFeatureFlagsDisable {

    @Rule
    public TogglzRule togglzRule = TogglzRule.allEnabled(LightblueMigrationFeatures.class);

    @Test
    public void testReadSourceEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.READ_SOURCE_ENTITY);
        Assert.assertFalse(LightblueMigrationFeatures.READ_SOURCE_ENTITY.isActive());
    }

    @Test
    public void testWriteSourceEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY);
        Assert.assertFalse(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY.isActive());
    }

    @Test
    public void testReadDestinationEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.READ_DESTINATION_ENTITY);
        Assert.assertFalse(LightblueMigrationFeatures.READ_DESTINATION_ENTITY.isActive());
    }

    @Test
    public void testWriteDestinationEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY);
        Assert.assertFalse(LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY.isActive());
    }

    @Test
    public void testCheckReadConsistencyFeature() {
        togglzRule.disable(LightblueMigrationFeatures.CHECK_READ_CONSISTENCY);
        Assert.assertFalse(LightblueMigrationFeatures.CHECK_READ_CONSISTENCY.isActive());
    }

    @Test
    public void testCheckWriteConsistencyFeature() {
        togglzRule.disable(LightblueMigrationFeatures.CHECK_WRITE_CONSISTENCY);
        Assert.assertFalse(LightblueMigrationFeatures.CHECK_WRITE_CONSISTENCY.isActive());
    }

}
