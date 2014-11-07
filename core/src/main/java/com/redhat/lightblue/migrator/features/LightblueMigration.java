package com.redhat.lightblue.migrator.features;

public class LightblueMigration {

	public static boolean readLegacyEntity() {
		return LightblueMigrationFeatures.READ_LEGACY_ENTITY.isActive();
	}

	public static boolean writeLegacyEntity() {
		return LightblueMigrationFeatures.WRITE_LEGACY_ENTITY.isActive();
	}

	public static boolean readLightblueEntity() {
		return LightblueMigrationFeatures.READ_LIGHTBLUE_ENTITY.isActive();
	}

	public static boolean writeLightblueEntity() {
		return LightblueMigrationFeatures.WRITE_LIGHTBLUE_ENTITY.isActive();
	}

	public static boolean readConsistencyEntity() {
		return LightblueMigrationFeatures.READ_CONSISTENCY_ENTITY.isActive();
	}

	public static boolean writeConsistencyEntity() {
		return LightblueMigrationFeatures.WRITE_CONSISTENCY_ENTITY.isActive();
	}

}
