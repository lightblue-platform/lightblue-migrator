package com.redhat.lightblue.migrator;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.redhat.lightblue.client.http.LightblueHttpClient;
import com.redhat.lightblue.client.request.data.DataFindRequest;

/**
 * Verify that generated and non generated jobs are retrieved for processing in desired proportions, i.e. that
 * migration and consistency checking can happen in parallel.
 *
 * @author mpatercz
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class WeightsTest {

    double acceptableThreshold = 0.03;

    @Mock LightblueHttpClient lightblueClient;

    Controller controller;

    @Test
    public void testGeneratedAndNongeneratedJobsAreQueriedInEqualProportions() throws Exception {
        MainConfiguration mainCfg = Mockito.spy(new MainConfiguration());
        mainCfg.setName("TestWeightMainConfiguration");

        MigrationConfiguration cfg = new MigrationConfiguration();
        cfg.setConfigurationName("TestWeightConfiguration");
        cfg.setThreadCount(1);
        cfg.setPeriod(null); // no consistency check
        cfg.setSleepIfNoJobs(false); // speed up things

        // tie mocked client to main conf
        Mockito.doReturn(lightblueClient).when(mainCfg).getLightblueClient();

        controller = Mockito.spy(new Controller(mainCfg));

        // handle conf reload
        Mockito.doReturn(new MigrationConfiguration[]{cfg}).when(controller).getMigrationConfigurations();
        Mockito.doReturn(cfg).when(lightblueClient).data(Mockito.isA(DataFindRequest.class), Mockito.eq(MigrationConfiguration.class));

        final int[] generated = new int[1];
        final int[] nongenerated = new int[1];
        final int[] any = new int[1];

        // observe migrationjob queries and count them by type
        Mockito.when(lightblueClient.data(Mockito.isA(DataFindRequest.class), Mockito.eq(MigrationJob[].class))).thenAnswer(new Answer<MigrationJob[]>() {

            @Override
            public MigrationJob[] answer(InvocationOnMock invocation) throws Throwable {
                DataFindRequest r = (DataFindRequest) invocation.getArguments()[0];

                String body = r.getBody();

                if (body.contains("{\"field\":\"generated\",\"op\":\"=\",\"rvalue\":true}")) {
                    generated[0]++;
                } else if (body.contains("{\"field\":\"generated\",\"op\":\"=\",\"rvalue\":false}")) {
                    nongenerated[0]++;
                } else {
                    any[0]++;
                }

                return null;
            }

        });

        controller.start();

        int loops = 0;
        while(any[0] < 5000) {
            Thread.sleep(100);

            if (loops++ > 10000) {
                throw new RuntimeException("Something's wrong, it shouldn't take that long");
            }
        }

        controller.setStopped();

        while(controller.isAlive()) {
            Thread.sleep(10);
        }

        System.out.println("generated="+generated[0]);
        System.out.println("nongenerated="+nongenerated[0]);
        System.out.println("any="+any[0]);

        Assert.assertTrue("Generated + nongenerated query count should be equal or so to any query count", Math.abs(generated[0]+nongenerated[0]-any[0]) < acceptableThreshold*any[0]);
        Assert.assertTrue("Generated - nongenerated query count should be zero or so", Math.abs(generated[0]-nongenerated[0]) < acceptableThreshold*any[0]);
    }
}
