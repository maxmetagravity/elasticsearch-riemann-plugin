package io.searchbox.service.riemann;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.client.RiemannClient;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 * @author ferhat
 */
public class NodeStatsRiemannEvent {

    private RiemannClient riemannClient;
    private NodeStats nodeStats;
    private String hostDefinition;

    public NodeStatsRiemannEvent(RiemannClient riemannClient, NodeStats nodeStats, String hostDefinition) {
        this.riemannClient = riemannClient;
        this.nodeStats = nodeStats;
        this.hostDefinition = hostDefinition;
    }

    private Proto.Event heapUsedEvent() {
        Long heapUsed = nodeStats.getJvm().getMem().getHeapUsed().getBytes();
        return riemannClient.event().host(hostDefinition).
                service("heap used").state("warning").metric(heapUsed).build();
    }

    private Proto.Event heapCommittedEvent() {
        ByteSizeValue heapCommitted = nodeStats.getJvm().getMem().getHeapCommitted();
        Long heapCommittedValue = heapCommitted.getGb() == 0 ? heapCommitted.getMb() : heapCommitted.getGb();
        return riemannClient.event().host(hostDefinition).
                service("heap committed").state("warning").metric(heapCommittedValue).build();
    }

    public void sendEvents() {
        riemannClient.sendEvents(heapCommittedEvent(), heapUsedEvent());
    }
}
