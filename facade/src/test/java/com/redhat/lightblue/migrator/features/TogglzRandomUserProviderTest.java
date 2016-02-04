package com.redhat.lightblue.migrator.features;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.togglz.core.Feature;
import org.togglz.core.manager.FeatureManager;
import org.togglz.core.manager.FeatureManagerBuilder;
import org.togglz.core.repository.FeatureState;
import org.togglz.core.repository.mem.InMemoryStateRepository;

/**
 *
 * @author nmalik
 */
public class TogglzRandomUserProviderTest {
    /**
     * Number of iterations.
     */
    private static final int ITERATIONS = 100000;

    /**
     * % error from target that's acceptable in the test.
     */
    private static final double ACCEPTABLE_PERCENTAGE_ERROR = 0.05;

    /**
     * Just need a feature to work with.
     */
    private static final Feature FEATURE = LightblueMigrationFeatures.READ_DESTINATION_ENTITY;

    private FeatureManager featureManager;

    @Before
    public void setup() {
        featureManager = new FeatureManagerBuilder()
                .featureEnum(LightblueMigrationFeatures.class)
                .stateRepository(new InMemoryStateRepository())
                .userProvider(new TogglzRandomUserProvider())
                .build();
    }

    @Test
    public void disabled() {
        featureManager.setFeatureState(new FeatureState(FEATURE, false));

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            if (featureManager.isActive(FEATURE)) {
                count++;
            }
        }

        Assert.assertEquals(0, count);
    }

    @Test
    public void enabled() {
        featureManager.setFeatureState(new FeatureState(FEATURE, true));

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            if (featureManager.isActive(FEATURE)) {
                count++;
            }
        }

        Assert.assertEquals(ITERATIONS, count);
    }

    @Test
    public void disabled_0percent() {
        int targetPercentage = 0;

        {
            FeatureState featureState = new FeatureState(FEATURE, false);
            featureState.setStrategyId("gradual");
            featureState.setParameter("percentage", String.valueOf(targetPercentage));
            featureManager.setFeatureState(featureState);
        }

        {
            // verify the state
            FeatureState featureState = featureManager.getFeatureState(FEATURE);
            Assert.assertFalse(featureState.isEnabled());
            Assert.assertEquals("gradual", featureState.getStrategyId());
            Assert.assertEquals(String.valueOf(targetPercentage), featureState.getParameter("percentage"));
        }

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            // re-init for each check to set a new random username
            TogglzRandomUsername.init();
            if (featureManager.isActive(FEATURE)) {
                count++;
            }
        }

        Assert.assertEquals(0, count);
    }

    @Test
    public void enabled_0percent() {
        int targetPercentage = 0;

        {
            FeatureState featureState = new FeatureState(FEATURE, true);
            featureState.setStrategyId("gradual");
            featureState.setParameter("percentage", String.valueOf(targetPercentage));
            featureManager.setFeatureState(featureState);
        }

        {
            // verify the state
            FeatureState featureState = featureManager.getFeatureState(FEATURE);
            Assert.assertTrue(featureState.isEnabled());
            Assert.assertEquals("gradual", featureState.getStrategyId());
            Assert.assertEquals(String.valueOf(targetPercentage), featureState.getParameter("percentage"));
        }

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            // re-init for each check to set a new random username
            TogglzRandomUsername.init();
            if (featureManager.isActive(FEATURE)) {
                count++;
            }
        }

        Assert.assertEquals(0, count);
    }

    @Test
    public void enabled_10percent() {
        int targetPercentage = 10;

        {
            FeatureState featureState = new FeatureState(FEATURE, true);
            featureState.setStrategyId("gradual");
            featureState.setParameter("percentage", String.valueOf(targetPercentage));
            featureManager.setFeatureState(featureState);
        }

        {
            // verify the state
            FeatureState featureState = featureManager.getFeatureState(FEATURE);
            Assert.assertTrue(featureState.isEnabled());
            Assert.assertEquals("gradual", featureState.getStrategyId());
            Assert.assertEquals(String.valueOf(targetPercentage), featureState.getParameter("percentage"));
        }

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            // re-init for each check to set a new random username
            TogglzRandomUsername.init();
            if (featureManager.isActive(FEATURE)) {
                count++;
            }
        }

        int target = ITERATIONS * targetPercentage / 100;

        Assert.assertTrue("should have been close to " + target + " but was " + count,
                target - target * ACCEPTABLE_PERCENTAGE_ERROR < count
                && count < target + target * ACCEPTABLE_PERCENTAGE_ERROR);
    }

    @Test
    public void enabled_50percent() {
        int targetPercentage = 50;

        {
            FeatureState featureState = new FeatureState(FEATURE, true);
            featureState.setStrategyId("gradual");
            featureState.setParameter("percentage", String.valueOf(targetPercentage));
            featureManager.setFeatureState(featureState);
        }

        {
            // verify the state
            FeatureState featureState = featureManager.getFeatureState(FEATURE);
            Assert.assertTrue(featureState.isEnabled());
            Assert.assertEquals("gradual", featureState.getStrategyId());
            Assert.assertEquals(String.valueOf(targetPercentage), featureState.getParameter("percentage"));
        }

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            // re-init for each check to set a new random username
            TogglzRandomUsername.init();
            if (featureManager.isActive(FEATURE)) {
                count++;
            }
        }

        int target = ITERATIONS * targetPercentage / 100;

        Assert.assertTrue("should have been close to " + target + " but was " + count,
                target - target * ACCEPTABLE_PERCENTAGE_ERROR < count
                && count < target + target * ACCEPTABLE_PERCENTAGE_ERROR);
    }

    @Test
    public void enabled_100percent() {
        int targetPercentage = 100;

        {
            FeatureState featureState = new FeatureState(FEATURE, true);
            featureState.setStrategyId("gradual");
            featureState.setParameter("percentage", String.valueOf(targetPercentage));
            featureManager.setFeatureState(featureState);
        }

        {
            // verify the state
            FeatureState featureState = featureManager.getFeatureState(FEATURE);
            Assert.assertTrue(featureState.isEnabled());
            Assert.assertEquals("gradual", featureState.getStrategyId());
            Assert.assertEquals(String.valueOf(targetPercentage), featureState.getParameter("percentage"));
        }

        int count = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            // re-init for each check to set a new random username
            TogglzRandomUsername.init();
            if (featureManager.isActive(FEATURE)) {
                count++;
            }
        }

        Assert.assertEquals(ITERATIONS, count);
    }
}
