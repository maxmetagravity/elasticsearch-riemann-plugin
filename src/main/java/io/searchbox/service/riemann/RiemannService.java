package io.searchbox.service.riemann;

import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.client.RiemannTcpClient;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RiemannService extends AbstractLifecycleComponent<RiemannService> {

    private final ClusterService clusterService;
    private NodeService nodeService;
    private final String riemannHost;
    private final Integer riemannPort;
    private final TimeValue riemannRefreshInternal;

    private volatile Thread riemannReporterThread;
    private volatile boolean closed;
    private final String clusterName;
    private final RiemannClient riemannClient;
    private final TransportClusterHealthAction transportClusterHealthAction;

    @Inject
    public RiemannService(Settings settings, ClusterService clusterService, NodeService nodeService, TransportClusterHealthAction transportClusterHealthAction) {
        super(settings);
        this.clusterService = clusterService;
        this.nodeService = nodeService;
        riemannRefreshInternal = settings.getAsTime("metrics.riemann.every", TimeValue.timeValueSeconds(1));
        riemannHost = settings.get("metrics.riemann.host", "localhost");
        riemannPort = settings.getAsInt("metrics.riemann.port", 5555);
        clusterName = settings.get("cluster.name");
        riemannClient = new RiemannClient(new InetSocketAddress(riemannHost, riemannPort));
        this.transportClusterHealthAction = transportClusterHealthAction;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        if (riemannHost != null && riemannHost.length() > 0) {
            riemannReporterThread = EsExecutors.daemonThreadFactory(settings, "riemann_reporter").newThread(new RiemannReporterThread());
            riemannReporterThread.start();
            logger.info("Riemann reporting triggered every [{}] to host [{}:{}]", riemannRefreshInternal, riemannHost, riemannPort);
        } else {
            logger.error("Riemann reporting disabled, no riemann host configured");
        }
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        if (closed) {
            return;
        }
        if (riemannReporterThread != null) {
            riemannReporterThread.interrupt();
        }
        closed = true;
        logger.info("Riemann reporter stopped");
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }

    public class RiemannReporterThread implements Runnable {

        public void run() {
            while (!closed) {
                DiscoveryNode node = clusterService.localNode();
                boolean isClusterStarted = clusterService.lifecycleState().equals(Lifecycle.State.STARTED);
                if (isClusterStarted && node != null) {

                    final String eventName = clusterName + ":" + node.name();

                    transportClusterHealthAction.execute(new ClusterHealthRequest(), new ActionListener<ClusterHealthResponse>() {
                        @Override
                        public void onResponse(ClusterHealthResponse clusterIndexHealths) {
                            riemannClient.event().host(eventName).service("Cluster Health").description("cluster_health")
                                    .state(clusterIndexHealths.getStatus().name().toLowerCase()).send();
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            riemannClient.event().host(eventName).service("Cluster Health").description("cluster_health").state("error").send();
                        }
                    });

                    NodeStats nodeStats = nodeService.stats(true, true, true, true, true, true, true, true, true);

                    Long heapUsed = nodeStats.getJvm().getMem().getHeapUsed().getBytes();
                    Long heapCommitted = nodeStats.getJvm().getMem().getHeapCommitted().getGb();

                    riemannClient.event().host(eventName).
                            service("heap used").state("warning").metric(heapUsed).send();
                    riemannClient.event().host(eventName).
                            service("heap commited").state("warning").metric(heapCommitted).send();
                    riemannClient.event().host(eventName).service("docs").state("warning").metric(nodeStats.getIndices().docs().count()).send();
                    riemannClient.event().host(eventName).service("GC").state("warning").metric(nodeStats.jvm().getGc().collectionCount())
                            .send();
                } else {
                    if (node != null) {
                        logger.debug("[{}]/[{}] is not started", node.getId(), node.getName());
                    } else {
                        logger.debug("Node is null!");
                    }
                }

                try {
                    Thread.sleep(riemannRefreshInternal.millis());
                } catch (InterruptedException e1) {
                }
            }
        }
    }
}
