package com.redhat.lightblue.migrator.features;

public class LightblueMigration {

    public static boolean shouldReadSourceEntity() {
        return LightblueMigrationFeatures.READ_SOURCE_ENTITY.isActive();
    }

    public static boolean shouldWriteSourceEntity() {
        return LightblueMigrationFeatures.WRITE_SOURCE_ENTITY.isActive();
    }

    public static boolean shouldReadDestinationEntity() {
        return LightblueMigrationFeatures.READ_DESTINATION_ENTITY.isActive();
    }

    public static boolean shouldWriteDestinationEntity() {
        return LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY.isActive();
    }

    public static boolean shouldReadConsistencyEntity() {
        return LightblueMigrationFeatures.READ_CONSISTENCY_ENTITY.isActive();
    }

    public static boolean shouldWriteConsistencyEntity() {
        return LightblueMigrationFeatures.WRITE_CONSISTENCY_ENTITY.isActive();
    }

}
