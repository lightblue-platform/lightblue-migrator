package com.redhat.lightblue.migrator.features;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.togglz.junit.TogglzRule;

public class TestLightblueMigrationDisable {

    @Rule
    public TogglzRule togglzRule = TogglzRule.allEnabled(LightblueMigrationFeatures.class);

    @Test
    public void testReadSourceEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.READ_SOURCE_ENTITY);
        Assert.assertFalse(LightblueMigration.shouldReadSourceEntity());
    }

    @Test
    public void testWriteSourceEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY);
        Assert.assertFalse(LightblueMigration.shouldWriteSourceEntity());
    }

    @Test
    public void testReadDestinationEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.READ_DESTINATION_ENTITY);
        Assert.assertFalse(LightblueMigration.shouldReadDestinationEntity());
    }

    @Test
    public void testWriteDestinationEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY);
        Assert.assertFalse(LightblueMigration.shouldWriteDestinationEntity());
    }

    @Test
    public void testCheckReadConsistencyFeature() {
        togglzRule.disable(LightblueMigrationFeatures.CHECK_READ_CONSISTENCY);
        Assert.assertFalse(LightblueMigration.shouldCheckReadConsistency());
    }

    @Test
    public void testCheckWriteConsistencyFeature() {
        togglzRule.disable(LightblueMigrationFeatures.CHECK_WRITE_CONSISTENCY);
        Assert.assertFalse(LightblueMigration.shouldCheckWriteConsistency());
    }

}
