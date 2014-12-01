package com.redhat.lightblue.migrator.features;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.togglz.junit.TogglzRule;

public class TestLightblueFeatureFlagsDisable {

    @Rule
    public TogglzRule togglzRule = TogglzRule.allEnabled(LightblueMigrationFeatures.class);

    @Test
    public void testReadLegacyEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.READ_LEGACY_ENTITY);
        Assert.assertFalse(LightblueMigrationFeatures.READ_LEGACY_ENTITY.isActive());
    }

    @Test
    public void testWriteLegacyEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.WRITE_LEGACY_ENTITY);
        Assert.assertFalse(LightblueMigrationFeatures.WRITE_LEGACY_ENTITY.isActive());
    }

    @Test
    public void testReadLightblueEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.READ_LIGHTBLUE_ENTITY);
        Assert.assertFalse(LightblueMigrationFeatures.READ_LIGHTBLUE_ENTITY.isActive());
    }

    @Test
    public void testWriteLightblueEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.WRITE_LIGHTBLUE_ENTITY);
        Assert.assertFalse(LightblueMigrationFeatures.WRITE_LIGHTBLUE_ENTITY.isActive());
    }

    @Test
    public void testReadConsistencyEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.READ_CONSISTENCY_ENTITY);
        Assert.assertFalse(LightblueMigrationFeatures.READ_CONSISTENCY_ENTITY.isActive());
    }

    @Test
    public void testWriteConsistencyEntityFeature() {
        togglzRule.disable(LightblueMigrationFeatures.WRITE_CONSISTENCY_ENTITY);
        Assert.assertFalse(LightblueMigrationFeatures.WRITE_CONSISTENCY_ENTITY.isActive());
    }

}
