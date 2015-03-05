package com.redhat.lightblue.migrator.test;

import org.togglz.junit.TogglzRule;

import com.redhat.lightblue.migrator.features.LightblueMigrationFeatures;

/**
 * Configure togglz switches to map to migration phases for junit.
 *
 * @author mpatercz
 *
 */
public abstract class LightblueMigrationPhase {

    /**
     *
     * @param togglzRule with all features disabled
     */
    public static void initialPhase(TogglzRule togglzRule) {
        togglzRule.enable(LightblueMigrationFeatures.READ_SOURCE_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY);
    }

    /**
     *
     * @param togglzRule with all features disabled
     */
    public static void dualWritePhase(TogglzRule togglzRule) {
        togglzRule.enable(LightblueMigrationFeatures.READ_SOURCE_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.CHECK_WRITE_CONSISTENCY);
    }

    /**
     *
     * @param togglzRule with all features disabled
     */
    public static void dualReadPhase(TogglzRule togglzRule) {
        togglzRule.enable(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.READ_SOURCE_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.READ_DESTINATION_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.CHECK_WRITE_CONSISTENCY);
        togglzRule.enable(LightblueMigrationFeatures.CHECK_READ_CONSISTENCY);
    }

    /**
     *
     * @param togglzRule with all features disabled
     */
    public static void lightblueProxyPhase(TogglzRule togglzRule) {
        togglzRule.enable(LightblueMigrationFeatures.READ_DESTINATION_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY);
    }
}
