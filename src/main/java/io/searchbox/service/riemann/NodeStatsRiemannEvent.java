package io.searchbox.service.riemann;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.client.RiemannClient;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.fs.FsStats;

import java.util.*;

/**
 * @author ferhat
 */
public class NodeStatsRiemannEvent {

    private RiemannClient riemannClient;
    private String hostDefinition;
    private Settings settings;
    private List<Proto.Event> eventList;
    private static NodeStatsRiemannEvent nodeStatsRiemannEvent;
    private Map<String, Long> deltaMap = new HashMap<String, Long>();

    public static NodeStatsRiemannEvent getNodeStatsRiemannEvent(RiemannClient riemannClient, Settings settings, String hostDefinition) {
        if (nodeStatsRiemannEvent == null) {
            nodeStatsRiemannEvent = new NodeStatsRiemannEvent(riemannClient, settings, hostDefinition);
        }
        return nodeStatsRiemannEvent;
    }

    private NodeStatsRiemannEvent(RiemannClient riemannClient, Settings settings, String hostDefinition) {
        this.riemannClient = riemannClient;
        this.hostDefinition = hostDefinition;
        this.settings = settings;

        // init required delta instead of null check
        deltaMap.put("index_rate", 0L);
        deltaMap.put("query_rate", 0L);
        deltaMap.put("fetch_rate", 0L);

    }

    public void sendEvents(NodeStats nodeStats) {
        eventList = new ArrayList<Proto.Event>();

        if (settings.getAsBoolean("metrics.riemann.heap_ratio", true)) {
            eventList.add(heapRatio(nodeStats));
        }

        if (settings.getAsBoolean("metrics.riemann.current_query_rate", true)) {
            eventList.add(currentQueryRate(nodeStats));
        }

        if (settings.getAsBoolean("metrics.riemann.current_fetch_rate", true)) {
            eventList.add(currentFetchRate(nodeStats));
        }

        if (settings.getAsBoolean("metrics.riemann.current_indexing_rate", true)) {
            eventList.add(currentIndexingRate(nodeStats));
        }

        if (settings.getAsBoolean("metrics.riemann.total_thread_count", true)) {
            eventList.add(totalThreadCount(nodeStats));
        }

        if (settings.getAsBoolean("metrics.riemann.system_load", true)) {
            //eventList.add(systemLoadOne(nodeStats));
        }

        if (settings.getAsBoolean("metrics.riemann.system_memory_usage", true)) {
            //eventList.add(systemMemory(nodeStats));
        }

        if (settings.getAsBoolean("metrics.riemann.disk_usage", true)) {
            //systemFile(nodeStats);
        }

        riemannClient.sendEvents(eventList);
    }

    private Proto.Event currentIndexingRate(NodeStats nodeStats) {
        long indexCount = nodeStats.indices().indexing().total().getIndexCount();
        long delta = deltaMap.get("index_rate");
        long indexingCurrent = indexCount - delta;
        deltaMap.put("index_rate", indexCount);
        return riemannClient.event().host(hostDefinition).
                service("Current Indexing Rate").description("current_indexing_rate").state(getState(indexingCurrent, 300, 1000)).metric(indexingCurrent).build();
    }

    private Proto.Event heapRatio(NodeStats nodeStats) {
        long heapUsed = nodeStats.getJvm().getMem().getHeapUsed().getBytes();
        long heapCommitted = nodeStats.getJvm().getMem().getHeapCommitted().getBytes();
        long heapRatio = (heapUsed * 100) / heapCommitted;
        return riemannClient.event().host(hostDefinition).
                service("Heap Usage Ratio %").description("heap_usage_ratio").state(getState(heapRatio, 85, 95)).metric(heapRatio).build();
    }

    private Proto.Event currentQueryRate(NodeStats nodeStats) {
        long queryCount = nodeStats.indices().search().total().getQueryCount();

        long delta = deltaMap.get("query_rate");
        long queryCurrent = queryCount - delta;
        deltaMap.put("query_rate", queryCount);

        return riemannClient.event().host(hostDefinition).
                service("Current Query Rate").description("current_query_rate").state(getState(queryCurrent, 50, 70)).metric(queryCurrent).build();
    }

    private Proto.Event currentFetchRate(NodeStats nodeStats) {
        long fetchCount = nodeStats.indices().search().total().getFetchCount();
        long delta = deltaMap.get("fetch_rate");
        long fetchCurrent = fetchCount - delta;
        deltaMap.put("fetch_rate", fetchCount);
        return riemannClient.event().host(hostDefinition).
                service("Current Fetch Rate").description("current_fetch_rate").state(getState(fetchCurrent, 50, 70)).metric(fetchCurrent).build();
    }

    private Proto.Event totalThreadCount(NodeStats nodeStats) {
        int threadCount = nodeStats.getJvm().getThreads().getCount();
        return riemannClient.event().host(hostDefinition).
                service("Total Thread Count").description("total_thread_count").state(getState(threadCount, 150, 200)).metric(threadCount).build();
    }

    private Proto.Event systemLoadOne(NodeStats nodeStats) {
        double[] systemLoad = nodeStats.getOs().getLoadAverage();
        return riemannClient.event().host(hostDefinition).
                service("System Load(1m)").description("system_load").state(getState((long) systemLoad[0], 2, 5)).metric(systemLoad[0]).build();
    }

    private Proto.Event systemMemory(NodeStats nodeStats) {
        short memoryUsedPercentage = nodeStats.getOs().getMem().getUsedPercent();
        return riemannClient.event().host(hostDefinition).
                service("System Memory Usage %").description("system_memory_usage").state(getState(memoryUsedPercentage, 80, 90)).metric(memoryUsedPercentage).build();

    }

    private void systemFile(NodeStats nodeStats) {
        for (FsStats.Info info : nodeStats.getFs()) {
            long free = info.getFree().getBytes();
            long total = info.getTotal().getBytes();
            long usageRatio = ((total - free) * 100) / total;
            eventList.add(riemannClient.event().host(hostDefinition).
                    service("Disk Usage %").description("system_disk_usage").state(getState(usageRatio, 80, 90)).metric(usageRatio).build());
        }
    }

    private String getState(long param, long ok, long warning) {
        String state;
        if (param < ok) {
            state = "ok";
        } else if (param < warning) {
            state = "warning";
        } else {
            state = "error";
        }
        return state;
    }
}
