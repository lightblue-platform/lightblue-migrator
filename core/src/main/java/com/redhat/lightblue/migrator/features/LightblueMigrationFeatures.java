package com.redhat.lightblue.migrator.features;

import org.togglz.core.Feature;
import org.togglz.core.annotation.EnabledByDefault;
import org.togglz.core.annotation.Label;
import org.togglz.core.context.FeatureContext;

public enum LightblueMigrationFeatures implements Feature {

    @EnabledByDefault
    @Label("Read Legacy Entity")
    READ_LEGACY_ENTITY,

    @EnabledByDefault
    @Label("Write Legacy Entity")
    WRITE_LEGACY_ENTITY,
    
    @Label("Read lightblue Entity")
    READ_LIGHTBLUE_ENTITY,
    
    @Label("Write lightblue Entity")
    WRITE_LIGHTBLUE_ENTITY,
    
    @Label("Read consistency Entity")
    READ_CONSISTENCY_ENTITY,
    
    @Label("Write consistency Entity")
    WRITE_CONSISTENCY_ENTITY,
    ;
    
    public boolean isActive() {
        return FeatureContext.getFeatureManager().isActive(this);
    }
}
