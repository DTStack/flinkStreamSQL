package com.dtstack.flink.sql.environment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({StreamGraphGenerator.class, MiniCluster.class, MyLocalStreamEnvironment.class})
public class MyLocalStreamEnvironmentTest {

    @Test
    public void testExecute() throws Exception {
        MyLocalStreamEnvironment myLocalStreamEnvironment = new MyLocalStreamEnvironment();

        StreamGraph streamGraph = mock(StreamGraph.class);
        JobGraph jobGraph = mock(JobGraph.class);
        when(jobGraph.getJobConfiguration()).thenReturn(new Configuration());
        when(jobGraph.getMaximumParallelism()).thenReturn(1);
        when(streamGraph.getJobGraph()).thenReturn(jobGraph);

        MiniCluster miniCluster = mock(MiniCluster.class);
        PowerMockito.suppress(MemberMatcher.methods(MiniCluster.class, "start"));
        PowerMockito.whenNew(MiniCluster.class).withArguments(anyObject()).thenReturn(miniCluster);
        CompletableFuture future = mock(CompletableFuture.class);
        when(miniCluster.getRestAddress()).thenReturn(future);
        URI uri = new URI("http://localhost:8080");
        when(future.get()).thenReturn(uri);
        when(miniCluster.executeJobBlocking(anyObject())).thenReturn(null);
        myLocalStreamEnvironment.execute(streamGraph);
    }
}
