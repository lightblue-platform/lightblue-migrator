package com.redhat.lightblue.migrator.features;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.togglz.junit.TogglzRule;

import com.redhat.lightblue.migrator.features.LightblueMigration;
import com.redhat.lightblue.migrator.features.LightblueMigrationFeatures;

public class TestLightblueMigrationDisable {
	
  @Rule
  public TogglzRule togglzRule = TogglzRule.allEnabled(LightblueMigrationFeatures.class);
		
	@Test
	public void testReadLegacyEntityFeature() {
		togglzRule.disable(LightblueMigrationFeatures.READ_LEGACY_ENTITY);
		Assert.assertFalse(LightblueMigration.readLegacyEntity());
	}
	
	@Test
	public void testWriteLegacyEntityFeature() {
		togglzRule.disable(LightblueMigrationFeatures.WRITE_LEGACY_ENTITY);
		Assert.assertFalse(LightblueMigration.writeLegacyEntity());
	}
	
	@Test
	public void testReadLightblueEntityFeature() {
		togglzRule.disable(LightblueMigrationFeatures.READ_LIGHTBLUE_ENTITY);
		Assert.assertFalse(LightblueMigration.readLightblueEntity());
	}
	
	@Test
	public void testWriteLightblueEntityFeature() {
		togglzRule.disable(LightblueMigrationFeatures.WRITE_LIGHTBLUE_ENTITY);
		Assert.assertFalse(LightblueMigration.writeLightblueEntity());
	}
	
	@Test
	public void testReadConsistencyEntityFeature() {
		togglzRule.disable(LightblueMigrationFeatures.READ_CONSISTENCY_ENTITY);
		Assert.assertFalse(LightblueMigration.readConsistencyEntity());
	}
	
	@Test
	public void testWriteConsistencyEntityFeature() {
		togglzRule.disable(LightblueMigrationFeatures.WRITE_CONSISTENCY_ENTITY);
		Assert.assertFalse(LightblueMigration.writeConsistencyEntity());
	}

}

