package io.searchbox.service.riemann;

import com.aphyr.riemann.client.RiemannClient;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.service.NodeService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RiemannService extends AbstractLifecycleComponent<RiemannService> {

    private final ClusterService clusterService;
    private NodeService nodeService;
    private final String riemannHost;
    private final Integer riemannPort;
    private final TimeValue riemannRefreshInternal;

    private volatile ExecutorService riemannReporterThreadExecutorService;
    private final String clusterName;
    private RiemannClient riemannClient;
    private final TransportClusterHealthAction transportClusterHealthAction;
    private String[] tags;

    private boolean open = true;

    Timer timer = new Timer();

    @Inject
    public RiemannService(Settings settings, ClusterService clusterService, NodeService nodeService, TransportClusterHealthAction transportClusterHealthAction) {
        super(settings);
        this.clusterService = clusterService;
        this.nodeService = nodeService;
        riemannRefreshInternal = settings.getAsTime("metrics.riemann.every", TimeValue.timeValueSeconds(1));
        riemannHost = settings.get("metrics.riemann.host", "localhost");
        riemannPort = settings.getAsInt("metrics.riemann.port", 5555);
        clusterName = settings.get("cluster.name");
        tags = settings.getAsArray("metrics.riemann.tags", new String[]{clusterName});
        try {
            riemannClient = RiemannClient.udp(new InetSocketAddress(riemannHost, riemannPort));
        } catch (IOException e) {
            logger.error("Can not create Riemann UDP connection", e);
        }
        this.transportClusterHealthAction = transportClusterHealthAction;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        try {
            riemannClient.connect();
        } catch (IOException e) {
            logger.error("Can not connect to Riemann", e);
        }
        if (riemannHost != null && riemannHost.length() > 0) {

            timer.scheduleAtFixedRate(new RiemannTask(), riemannRefreshInternal.millis(), riemannRefreshInternal.millis());

            logger.info("Riemann reporting triggered every [{}] to host [{}:{}]", riemannRefreshInternal, riemannHost, riemannPort);
        } else {
            logger.error("Riemann reporting disabled, no riemann host configured");
        }
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        try {
            riemannClient.disconnect();
            open = false;
        } catch (IOException e) {
            logger.error("Riemann connection can not be closed", e);
        }
        if (riemannReporterThreadExecutorService != null) {
            riemannReporterThreadExecutorService.shutdown();
        }
        logger.info("Riemann reporter stopped");
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }

    class RiemannTask extends TimerTask {

        @Override
        public void run() {
            logger.debug("running  RiemannTask");
            if (riemannClient.isConnected()) {
                logger.debug("getting data via discovery node...");
                DiscoveryNode node = clusterService.localNode();
                boolean isClusterStarted = clusterService.lifecycleState().equals(Lifecycle.State.STARTED);
                if (isClusterStarted && node != null) {

                    final String hostDefinition = clusterName + ":" + node.name();

                    if (settings.getAsBoolean("metrics.riemann.health", true)) {

                        transportClusterHealthAction.execute(new ClusterHealthRequest(), new ActionListener<ClusterHealthResponse>() {
                            @Override
                            public void onResponse(ClusterHealthResponse clusterIndexHealths) {
                                riemannClient.event().host(hostDefinition).service("Cluster Health").description("cluster_health").tags(tags)
                                        .state(RiemannUtils.getStateWithClusterInformation(clusterIndexHealths.getStatus().name())).send();
                            }

                            @Override
                            public void onFailure(Throwable throwable) {
                                riemannClient.event().host(hostDefinition).service("Cluster Health").description("cluster_health").tags(tags).state("critical").send();
                            }
                        });
                    }

                    NodeStats nodeStats = nodeService.stats();
                    NodeStatsRiemannEvent nodeStatsRiemannEvent = NodeStatsRiemannEvent.getNodeStatsRiemannEvent(riemannClient, settings, hostDefinition, clusterName, tags);
                    nodeStatsRiemannEvent.sendEvents(nodeStats);

                    logger.debug("event sent to riemann");

                } else {
                    if (node != null) {
                        logger.info("[{}]/[{}] is not started", node.getId(), node.getName());
                    } else {
                        logger.info("Node is null!");
                    }
                }
            }
        }
    }
}
