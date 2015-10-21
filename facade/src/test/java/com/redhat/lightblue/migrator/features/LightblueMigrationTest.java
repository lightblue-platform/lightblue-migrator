package com.redhat.lightblue.migrator.features;

import org.junit.Assert;
import org.junit.Test;
import org.togglz.core.Feature;
import org.togglz.core.context.FeatureContext;
import org.togglz.core.repository.FeatureState;

/**
 *
 * @author nmalik
 */
public class LightblueMigrationTest {
    private static final int ITERATIONS = 100000;
    private static final double ACCEPTABLE_PERCENTAGE_ERROR = 0.05;

    @Test
    public void shouldReadSourceEntity_false() {
        FeatureContext.getFeatureManager().setFeatureState(new FeatureState(LightblueMigrationFeatures.READ_SOURCE_ENTITY, false));

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            if (LightblueMigration.shouldReadSourceEntity()) {
                count++;
            }
        }

        Assert.assertEquals(0, count);
    }

    @Test
    public void shouldReadSourceEntity_true() {
        FeatureContext.getFeatureManager().setFeatureState(new FeatureState(LightblueMigrationFeatures.READ_SOURCE_ENTITY, true));

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            if (LightblueMigration.shouldReadSourceEntity()) {
                count++;
            }
        }

        Assert.assertEquals(ITERATIONS, count);
    }

    @Test
    public void shouldWriteSourceEntity_false() {
        FeatureContext.getFeatureManager().setFeatureState(new FeatureState(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY, false));

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            if (LightblueMigration.shouldWriteSourceEntity()) {
                count++;
            }
        }

        Assert.assertEquals(0, count);
    }

    @Test
    public void shouldWriteSourceEntity_true() {
        FeatureContext.getFeatureManager().setFeatureState(new FeatureState(LightblueMigrationFeatures.WRITE_SOURCE_ENTITY, true));

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            if (LightblueMigration.shouldWriteSourceEntity()) {
                count++;
            }
        }

        Assert.assertEquals(ITERATIONS, count);
    }

    @Test
    public void shouldReadDestinationEntity_0_disabled() {
        FeatureContext.getFeatureManager().setFeatureState(new FeatureState(LightblueMigrationFeatures.READ_DESTINATION_ENTITY, false));

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            if (LightblueMigration.shouldReadDestinationEntity()) {
                count++;
            }
        }

        Assert.assertEquals(0, count);
    }

    @Test
    public void shouldReadDestinationEntity_0_enabled() {
        int targetPercentage = 0;
        Feature feature = LightblueMigrationFeatures.READ_DESTINATION_ENTITY;

        {
            FeatureState featureState = new FeatureState(feature, true);
            featureState.setStrategyId("gradual");
            featureState.setParameter("percentage", String.valueOf(targetPercentage));
            FeatureContext.getFeatureManager().setFeatureState(featureState);
        }

        {
            // verify the state
            FeatureState featureState = FeatureContext.getFeatureManager().getFeatureState(feature);
            Assert.assertTrue(featureState.isEnabled());
            Assert.assertEquals("gradual", featureState.getStrategyId());
            Assert.assertEquals(String.valueOf(targetPercentage), featureState.getParameter("percentage"));
        }

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            // re-init for each check to set a new random username
            TogglzRandomUsername.init();
            if (LightblueMigration.shouldReadDestinationEntity()) {
                count++;
            }
        }

        Assert.assertEquals(0, count);
    }

    @Test
    public void shouldReadDestinationEntity_10() {
        int targetPercentage = 10;
        Feature feature = LightblueMigrationFeatures.READ_DESTINATION_ENTITY;

        {
            FeatureState featureState = new FeatureState(feature, true);
            featureState.setStrategyId("gradual");
            featureState.setParameter("percentage", String.valueOf(targetPercentage));
            FeatureContext.getFeatureManager().setFeatureState(featureState);
        }

        {
            // verify the state
            FeatureState featureState = FeatureContext.getFeatureManager().getFeatureState(feature);
            Assert.assertTrue(featureState.isEnabled());
            Assert.assertEquals("gradual", featureState.getStrategyId());
            Assert.assertEquals(String.valueOf(targetPercentage), featureState.getParameter("percentage"));
        }

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            // re-init for each check to set a new random username
            TogglzRandomUsername.init();
            if (LightblueMigration.shouldReadDestinationEntity()) {
                count++;
            }
        }

        int target = ITERATIONS * targetPercentage / 100;

        Assert.assertTrue("should have been close to " + target + " but was " + count,
                target - target * ACCEPTABLE_PERCENTAGE_ERROR < count
                && count < target + target * ACCEPTABLE_PERCENTAGE_ERROR);
    }

    @Test
    public void shouldReadDestinationEntity_50() {
        int targetPercentage = 50;
        Feature feature = LightblueMigrationFeatures.READ_DESTINATION_ENTITY;

        {
            FeatureState featureState = new FeatureState(feature, true);
            featureState.setStrategyId("gradual");
            featureState.setParameter("percentage", String.valueOf(targetPercentage));
            FeatureContext.getFeatureManager().setFeatureState(featureState);
        }

        {
            // verify the state
            FeatureState featureState = FeatureContext.getFeatureManager().getFeatureState(feature);
            Assert.assertTrue(featureState.isEnabled());
            Assert.assertEquals("gradual", featureState.getStrategyId());
            Assert.assertEquals(String.valueOf(targetPercentage), featureState.getParameter("percentage"));
        }

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            // re-init for each check to set a new random username
            TogglzRandomUsername.init();
            if (LightblueMigration.shouldReadDestinationEntity()) {
                count++;
            }
        }

        int target = ITERATIONS * targetPercentage / 100;

        Assert.assertTrue("should have been close to " + target + " but was " + count,
                target - target * ACCEPTABLE_PERCENTAGE_ERROR < count
                && count < target + target * ACCEPTABLE_PERCENTAGE_ERROR);
    }

    @Test
    public void shouldReadDestinationEntity_100() {
        int targetPercentage = 100;
        Feature feature = LightblueMigrationFeatures.READ_DESTINATION_ENTITY;

        {
            FeatureState featureState = new FeatureState(feature, true);
            featureState.setStrategyId("gradual");
            featureState.setParameter("percentage", String.valueOf(targetPercentage));
            FeatureContext.getFeatureManager().setFeatureState(featureState);
        }

        {
            // verify the state
            FeatureState featureState = FeatureContext.getFeatureManager().getFeatureState(feature);
            Assert.assertTrue(featureState.isEnabled());
            Assert.assertEquals("gradual", featureState.getStrategyId());
            Assert.assertEquals(String.valueOf(targetPercentage), featureState.getParameter("percentage"));
        }

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            // re-init for each check to set a new random username
            TogglzRandomUsername.init();
            if (LightblueMigration.shouldReadDestinationEntity()) {
                count++;
            }
        }

        Assert.assertEquals(ITERATIONS, count);
    }

    @Test
    public void shouldWriteDestinationEntity_0_disabled() {
        FeatureContext.getFeatureManager().setFeatureState(new FeatureState(LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY, false));

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            if (LightblueMigration.shouldWriteDestinationEntity()) {
                count++;
            }
        }

        Assert.assertEquals(0, count);
    }

    @Test
    public void shouldWriteDestinationEntity_0_enabled() {
        int targetPercentage = 0;
        Feature feature = LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY;

        {
            FeatureState featureState = new FeatureState(feature, true);
            featureState.setStrategyId("gradual");
            featureState.setParameter("percentage", String.valueOf(targetPercentage));
            FeatureContext.getFeatureManager().setFeatureState(featureState);
        }

        {
            // verify the state
            FeatureState featureState = FeatureContext.getFeatureManager().getFeatureState(feature);
            Assert.assertTrue(featureState.isEnabled());
            Assert.assertEquals("gradual", featureState.getStrategyId());
            Assert.assertEquals(String.valueOf(targetPercentage), featureState.getParameter("percentage"));
        }

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            // re-init for each check to set a new random username
            TogglzRandomUsername.init();
            if (LightblueMigration.shouldWriteDestinationEntity()) {
                count++;
            }
        }

        Assert.assertEquals(0, count);
    }

    @Test
    public void shouldWriteDestinationEntity_10() {
        int targetPercentage = 10;
        Feature feature = LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY;

        {
            FeatureState featureState = new FeatureState(feature, true);
            featureState.setStrategyId("gradual");
            featureState.setParameter("percentage", String.valueOf(targetPercentage));
            FeatureContext.getFeatureManager().setFeatureState(featureState);
        }

        {
            // verify the state
            FeatureState featureState = FeatureContext.getFeatureManager().getFeatureState(feature);
            Assert.assertTrue(featureState.isEnabled());
            Assert.assertEquals("gradual", featureState.getStrategyId());
            Assert.assertEquals(String.valueOf(targetPercentage), featureState.getParameter("percentage"));
        }

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            // re-init for each check to set a new random username
            TogglzRandomUsername.init();
            if (LightblueMigration.shouldWriteDestinationEntity()) {
                count++;
            }
        }

        int target = ITERATIONS * targetPercentage / 100;

        Assert.assertTrue("should have been close to " + target + " but was " + count,
                target - target * ACCEPTABLE_PERCENTAGE_ERROR < count
                && count < target + target * ACCEPTABLE_PERCENTAGE_ERROR);
    }

    @Test
    public void shouldWriteDestinationEntity_50() {
        int targetPercentage = 50;
        Feature feature = LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY;

        {
            FeatureState featureState = new FeatureState(feature, true);
            featureState.setStrategyId("gradual");
            featureState.setParameter("percentage", String.valueOf(targetPercentage));
            FeatureContext.getFeatureManager().setFeatureState(featureState);
        }

        {
            // verify the state
            FeatureState featureState = FeatureContext.getFeatureManager().getFeatureState(feature);
            Assert.assertTrue(featureState.isEnabled());
            Assert.assertEquals("gradual", featureState.getStrategyId());
            Assert.assertEquals(String.valueOf(targetPercentage), featureState.getParameter("percentage"));
        }

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            // re-init for each check to set a new random username
            TogglzRandomUsername.init();
            if (LightblueMigration.shouldWriteDestinationEntity()) {
                count++;
            }
        }

        int target = ITERATIONS * targetPercentage / 100;

        Assert.assertTrue("should have been close to " + target + " but was " + count,
                target - target * ACCEPTABLE_PERCENTAGE_ERROR < count
                && count < target + target * ACCEPTABLE_PERCENTAGE_ERROR);
    }

    @Test
    public void shouldWriteDestinationEntity_100() {
        int targetPercentage = 100;
        Feature feature = LightblueMigrationFeatures.WRITE_DESTINATION_ENTITY;

        {
            FeatureState featureState = new FeatureState(feature, true);
            featureState.setStrategyId("gradual");
            featureState.setParameter("percentage", String.valueOf(targetPercentage));
            FeatureContext.getFeatureManager().setFeatureState(featureState);
        }

        {
            // verify the state
            FeatureState featureState = FeatureContext.getFeatureManager().getFeatureState(feature);
            Assert.assertTrue(featureState.isEnabled());
            Assert.assertEquals("gradual", featureState.getStrategyId());
            Assert.assertEquals(String.valueOf(targetPercentage), featureState.getParameter("percentage"));
        }

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            // re-init for each check to set a new random username
            TogglzRandomUsername.init();
            if (LightblueMigration.shouldWriteDestinationEntity()) {
                count++;
            }
        }

        Assert.assertEquals(ITERATIONS, count);
    }

    @Test
    public void shouldCheckReadConsistency_true() {
        FeatureContext.getFeatureManager().setFeatureState(new FeatureState(LightblueMigrationFeatures.CHECK_READ_CONSISTENCY, true));

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            if (LightblueMigration.shouldCheckReadConsistency()) {
                count++;
            }
        }

        Assert.assertEquals(ITERATIONS, count);
    }

    @Test
    public void shouldCheckWriteConsistency_true() {
        FeatureContext.getFeatureManager().setFeatureState(new FeatureState(LightblueMigrationFeatures.CHECK_WRITE_CONSISTENCY, true));

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            if (LightblueMigration.shouldCheckWriteConsistency()) {
                count++;
            }
        }

        Assert.assertEquals(ITERATIONS, count);
    }
}
