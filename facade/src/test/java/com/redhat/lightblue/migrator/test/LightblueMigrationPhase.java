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

    public static void enableConsistencyChecks(boolean enable, TogglzRule togglzRule) {
        if (enable) {
            togglzRule.enable(LightblueMigrationFeatures.CHECK_WRITE_CONSISTENCY);
            togglzRule.enable(LightblueMigrationFeatures.CHECK_READ_CONSISTENCY);
        } else {
            togglzRule.disable(LightblueMigrationFeatures.CHECK_WRITE_CONSISTENCY);
            togglzRule.disable(LightblueMigrationFeatures.CHECK_READ_CONSISTENCY);
        }
    }

    /**
     *
     * @param togglzRule with all features disabled
     */
    public static void initialPhase(TogglzRule togglzRule) {
        togglzRule.enable(LightblueMigrationFeatures.READ_SOURCE_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY);
        enableConsistencyChecks(true, togglzRule);
    }

    /**
     *
     * @param togglzRule with all features disabled
     */
    public static void dualWritePhase(TogglzRule togglzRule) {
        togglzRule.enable(LightblueMigrationFeatures.READ_SOURCE_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY);
        enableConsistencyChecks(true, togglzRule);
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
        enableConsistencyChecks(true, togglzRule);
    }

    /**
     * Kinda proxy phase means:
     * 1) writes go to both legacy and Lightblue services
     * 1) reads go only to Lightblue service
     * 3) consistency checks are disabled
     *
     * For write operations, legacy is still going to be called first to ensure all generated data
     * (ids, timestamps, etc.) can be passed to shared store. Legacy is still the source of this shared information.
     *
     * All Lightblue timeouts have to be disabled for kinda proxy phase.
     *
     */
    public static void lightblueKindaProxyPhase(TogglzRule togglzRule) {
        togglzRule.enable(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.READ_DESTINATION_ENTITY);
        enableConsistencyChecks(false, togglzRule);
    }

    /**
     *
     * @param togglzRule with all features disabled
     */
    public static void lightblueProxyPhase(TogglzRule togglzRule) {
        togglzRule.enable(LightblueMigrationFeatures.READ_DESTINATION_ENTITY);
        togglzRule.enable(LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY);
        enableConsistencyChecks(true, togglzRule);
    }
}
