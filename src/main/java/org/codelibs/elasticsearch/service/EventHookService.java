package org.codelibs.elasticsearch.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes.Delta;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;

public class EventHookService extends
        AbstractLifecycleComponent<EventHookService> implements
        ClusterStateListener, LocalNodeMasterListener {

    private static final String EVENT_TYPE_UNKNOWN = "unknown";

    private static final String DEFAULT_EVENTHOOK_INDEX = ".eventhook";

    private static final int DEFAULT_EVENTHOOK_SIZE = 100;

    private static final String CLUSTER_EVENTHOOK_INDEX = "cluster.eventhook.index";

    private static final String CLUSTER_EVENTHOOK_SIZE = "cluster.eventhook.size";

    private static final String CLUSTER_EVENTHOOK_ENABLE = "cluster.eventhook.enable";

    private ClusterService clusterService;

    private ScriptService scriptService;

    private Client client;

    private ThreadPool threadPool;

    private volatile boolean isMaster = false;

    private String index;

    private int eventSize;

    @Inject
    public EventHookService(final Settings settings,
            final DynamicSettings dynamicSettings,
            final ClusterService clusterService, final Client client,
            final ScriptService scriptService, final ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.client = client;
        this.scriptService = scriptService;
        this.threadPool = threadPool;

        logger.info("Creating EventHookService");

        index = settings.get(CLUSTER_EVENTHOOK_INDEX, DEFAULT_EVENTHOOK_INDEX);
        eventSize = settings.getAsInt(CLUSTER_EVENTHOOK_SIZE,
                DEFAULT_EVENTHOOK_SIZE);

    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("Starting EventHookService");

        clusterService.add((ClusterStateListener) this);
        clusterService.add((LocalNodeMasterListener) this);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("Stopping EventHookService");

        clusterService.remove((LocalNodeMasterListener) this);
        clusterService.remove((ClusterStateListener) this);
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    //
    // ClusterStateListener
    //

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (settings.getAsBoolean(CLUSTER_EVENTHOOK_ENABLE, true)) {
            final String type = getEventType(event.source());
            if (logger.isDebugEnabled()) {
                logger.debug("Cluster Event: {}/{}: {}", index, type,
                        event.source());
            }
            invokeScript(type, event);
        }
    }

    private Map<String, Object> createEventVars() {
        final Map<String, Object> vars = new HashMap<String, Object>();
        vars.put("isMaster", isMaster);
        vars.put("client", client);
        vars.put("clusterService", clusterService);
        vars.put("nodes", new Nodes());
        vars.put("cluster", new Cluster());
        return vars;
    }

    private void invokeScript(final String type, final ClusterChangedEvent event) {
        if (clusterService.state().metaData().hasIndex(index)) {
            client.prepareSearch(index).setTypes(type, "all")
                    .setQuery(QueryBuilders.matchAllQuery()).setSize(eventSize)
                    .addSort("priority", SortOrder.ASC)
                    .execute(new ActionListener<SearchResponse>() {
                        @Override
                        public void onResponse(final SearchResponse response) {
                            final SearchHits hits = response.getHits();
                            final long totalHits = hits.getTotalHits();
                            if (totalHits == 0) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("No scripts for {} event.",
                                            type);
                                }
                                return;
                            }
                            if (logger.isDebugEnabled()) {
                                logger.debug("{}/{} scripts will be executed.",
                                        hits.getHits().length, totalHits);
                            }
                            final Map<String, Object> vars = createEventVars();
                            vars.put("eventType", type);
                            vars.put("event", new Event(event));
                            for (final SearchHit hit : hits.getHits()) {
                                final Map<String, Object> source = hit
                                        .getSource();
                                final Object lang = source.get("lang");
                                final Object script = source.get("script");
                                final ScriptType scriptType = getScriptType(source
                                        .get("script_type"));
                                if (lang != null && script != null
                                        && scriptType != null) {
                                    threadPool.generic().execute(
                                            new Runnable() {
                                                @Override
                                                public void run() {
                                                    try {
                                                        final CompiledScript compiledScript = scriptService.compile(
                                                                lang.toString(),
                                                                script.toString(),
                                                                scriptType);
                                                        scriptService.execute(
                                                                compiledScript,
                                                                vars);
                                                    } catch (final Exception e) {
                                                        logger.error(
                                                                "Failed to execute a script: \nlang: {}\nscript: {}\nscriptType: {}",
                                                                e, lang,
                                                                script,
                                                                scriptType);
                                                    }
                                                }
                                            });
                                }
                            }
                        }

                        @Override
                        public void onFailure(final Throwable e) {
                            if (e instanceof ClusterBlockException) {
                                logger.debug("Cluster is still blocked.", e);
                            } else if (e instanceof SearchPhaseExecutionException) {
                                logger.debug(
                                        "{} is not available yet. The event is {}:{}",
                                        e, index, type,
                                        event != null ? event.source() : "");
                            } else {
                                logger.error(
                                        "Failed to find scripts for an event hook.",
                                        e);
                            }
                        }
                    });
        } else {
            logger.error("Failed to check if {} exists.", index);

        }
    }

    protected String getEventType(final String source) {
        if (source != null && source.length() > 0) {
            return source.replaceAll("[\\(\\[].*", "").trim()
                    .replaceAll("[\\s\\-]", "_");
        }
        return EVENT_TYPE_UNKNOWN;
    }

    private static ScriptType getScriptType(final Object scriptType) {
        if (scriptType == null) {
            return ScriptType.INLINE;
        } else if ("INDEXED".equalsIgnoreCase(scriptType.toString())) {
            return ScriptType.INDEXED;
        } else if ("FILE".equalsIgnoreCase(scriptType.toString())) {
            return ScriptType.FILE;
        } else {
            return ScriptType.INLINE;
        }
    }

    //
    // LocalNodeMasterListener
    //

    @Override
    public void onMaster() {
        isMaster = true;

        invokeScript("on_master", null);
    }

    @Override
    public void offMaster() {
        isMaster = false;

        invokeScript("off_master", null);
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.MANAGEMENT;
    }

    public class Nodes {
        public NodeInfo[] nodeInfo(final String... nodesIds) {
            final NodesInfoResponse response = client.admin().cluster()
                    .prepareNodesInfo(nodesIds).execute().actionGet();
            return response.getNodes();
        }
    }

    public class Cluster {
        public DiscoveryNode getLocalNode() {
            return clusterService.state().getNodes().getLocalNode();
        }

        public String getPersistentSettings(final String key) {
            return clusterService.state().metaData().persistentSettings()
                    .get(key);
        }

        public boolean setPersistentSettings(final String key,
                final String value) {
            final ClusterUpdateSettingsResponse response = client
                    .admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(
                            "{\"" + key + "\":\"" + value + "\"}").execute()
                    .actionGet();
            return response.isAcknowledged();
        }

        public String getTransientSettings(final String key) {
            return clusterService.state().metaData().transientSettings()
                    .get(key);
        }

        public boolean setTransientSettings(final String key, final String value) {
            final ClusterUpdateSettingsResponse response = client
                    .admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings("{\"" + key + "\":\"" + value + "\"}")
                    .execute().actionGet();
            return response.isAcknowledged();
        }
    }

    public class Event {
        ClusterChangedEvent event;

        public Event(final ClusterChangedEvent event) {
            this.event = event;
        }

        public String source() {
            if (event == null) {
                return "";
            }
            return event.source();
        }

        public ClusterState state() {
            if (event == null) {
                return clusterService.state();
            }
            return event.state();
        }

        public ClusterState previousState() {
            if (event == null) {
                return state();
            }
            return event.previousState();
        }

        public boolean routingTableChanged() {
            if (event == null) {
                return false;
            }
            return event.routingTableChanged();
        }

        public boolean indexRoutingTableChanged(final String index) {
            if (event == null) {
                return false;
            }
            return event.indexRoutingTableChanged(index);
        }

        public List<String> indicesCreated() {
            if (event == null) {
                return ImmutableList.<String> of();
            }
            return event.indicesCreated();
        }

        public List<String> indicesDeleted() {
            if (event == null) {
                return ImmutableList.<String> of();
            }
            return event.indicesDeleted();
        }

        public boolean metaDataChanged() {
            if (event == null) {
                return false;
            }
            return event.metaDataChanged();
        }

        public boolean indexMetaDataChanged(final IndexMetaData current) {
            if (event == null) {
                return false;
            }
            return event.indexMetaDataChanged(current);
        }

        public boolean blocksChanged() {
            if (event == null) {
                return false;
            }
            return event.blocksChanged();
        }

        public boolean localNodeMaster() {
            if (event == null) {
                return state().nodes().localNodeMaster();
            }
            return event.localNodeMaster();
        }

        public Delta nodesDelta() {
            if (event == null) {
                return state().nodes().delta(previousState().nodes());
            }
            return event.nodesDelta();
        }

        public boolean nodesRemoved() {
            if (event == null) {
                return false;
            }
            return event.nodesRemoved();
        }

        public boolean nodesAdded() {
            if (event == null) {
                return false;
            }
            return event.nodesAdded();
        }

        public boolean nodesChanged() {
            if (event == null) {
                return false;
            }
            return event.nodesChanged();
        }

    }
}
