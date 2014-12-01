package com.redhat.lightblue.migrator.features;

public class LightblueMigration {

    public static boolean shouldReadLegacyEntity() {
        return LightblueMigrationFeatures.READ_LEGACY_ENTITY.isActive();
    }

    public static boolean shouldWriteLegacyEntity() {
        return LightblueMigrationFeatures.WRITE_LEGACY_ENTITY.isActive();
    }

    public static boolean shouldReadLightblueEntity() {
        return LightblueMigrationFeatures.READ_LIGHTBLUE_ENTITY.isActive();
    }

    public static boolean shouldWriteLightblueEntity() {
        return LightblueMigrationFeatures.WRITE_LIGHTBLUE_ENTITY.isActive();
    }

    public static boolean shouldReadConsistencyEntity() {
        return LightblueMigrationFeatures.READ_CONSISTENCY_ENTITY.isActive();
    }

    public static boolean shouldWriteConsistencyEntity() {
        return LightblueMigrationFeatures.WRITE_CONSISTENCY_ENTITY.isActive();
    }

}
