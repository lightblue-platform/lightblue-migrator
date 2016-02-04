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

    public static boolean shouldCheckReadConsistency() {
        return LightblueMigrationFeatures.CHECK_READ_CONSISTENCY.isActive();
    }

    public static boolean shouldCheckWriteConsistency() {
        return LightblueMigrationFeatures.CHECK_WRITE_CONSISTENCY.isActive();
    }

}
