package com.redhat.lightblue.migrator.features;

import org.togglz.core.Feature;
import org.togglz.core.annotation.EnabledByDefault;
import org.togglz.core.annotation.Label;
import org.togglz.core.context.FeatureContext;

public enum LightblueMigrationFeatures implements Feature {

    @EnabledByDefault
    @Label("Read Source Entity")
    READ_SOURCE_ENTITY,
    @EnabledByDefault
    @Label("Write Source Entity")
    WRITE_SOURCE_ENTITY,
    @Label("Read Destination Entity")
    READ_DESTINATION_ENTITY,
    @Label("Write Destination Entity")
    WRITE_DESTINATION_ENTITY,
    @Label("Read consistency Entity")
    READ_CONSISTENCY_ENTITY,
    @Label("Write consistency Entity")
    WRITE_CONSISTENCY_ENTITY,;

    public boolean isActive() {
        return FeatureContext.getFeatureManager().isActive(this);
    }
}
