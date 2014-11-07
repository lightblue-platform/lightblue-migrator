package com.redhat.lightblue.migrator.features;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.togglz.junit.TogglzRule;

import com.redhat.lightblue.migrator.features.LightblueMigration;
import com.redhat.lightblue.migrator.features.LightblueMigrationFeatures;

public class TestLightblueMigrationEnable {

	@Rule
	public TogglzRule togglzRule = TogglzRule.allDisabled(LightblueMigrationFeatures.class);

	@Test
	public void testReadLegacyEntityFeature() {
		togglzRule.enable(LightblueMigrationFeatures.READ_LEGACY_ENTITY);
		Assert.assertTrue(LightblueMigration.readLegacyEntity());
	}

	@Test
	public void testWriteLegacyEntityFeature() {
		togglzRule.enable(LightblueMigrationFeatures.WRITE_LEGACY_ENTITY);
		Assert.assertTrue(LightblueMigration.writeLegacyEntity());
	}

	@Test
	public void testReadLightblueEntityFeature() {
		togglzRule.enable(LightblueMigrationFeatures.READ_LIGHTBLUE_ENTITY);
		Assert.assertTrue(LightblueMigration.readLightblueEntity());
	}

	@Test
	public void testWriteLightblueEntityFeature() {
		togglzRule.enable(LightblueMigrationFeatures.WRITE_LIGHTBLUE_ENTITY);
		Assert.assertTrue(LightblueMigration.writeLightblueEntity());
	}

	@Test
	public void testReadConsistencyEntityFeature() {
		togglzRule.enable(LightblueMigrationFeatures.READ_CONSISTENCY_ENTITY);
		Assert.assertTrue(LightblueMigration.readConsistencyEntity());
	}

	@Test
	public void testWriteConsistencyEntityFeature() {
		togglzRule.enable(LightblueMigrationFeatures.WRITE_CONSISTENCY_ENTITY);
		Assert.assertTrue(LightblueMigration.writeConsistencyEntity());
	}

}
