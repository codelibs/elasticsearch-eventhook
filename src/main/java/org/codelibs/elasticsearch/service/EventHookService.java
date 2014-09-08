package org.codelibs.elasticsearch.service;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
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

    private volatile boolean isMaster = false;

    private String index;

    private int eventSize;

    private Nodes nodes;

    private Cluster cluster;

    @Inject
    public EventHookService(final Settings settings,
            final DynamicSettings dynamicSettings,
            final ClusterService clusterService, final Client client,
            final ScriptService scriptService) {
        super(settings);
        this.clusterService = clusterService;
        this.client = client;
        this.scriptService = scriptService;

        logger.info("Creating EventHookService");

        index = settings.get(CLUSTER_EVENTHOOK_INDEX, DEFAULT_EVENTHOOK_INDEX);
        eventSize = settings.getAsInt(CLUSTER_EVENTHOOK_SIZE,
                DEFAULT_EVENTHOOK_SIZE);

        nodes = new Nodes();
        cluster = new Cluster();
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        logger.info("Starting SampleService");

        clusterService.add((ClusterStateListener) this);
        clusterService.add((LocalNodeMasterListener) this);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        logger.info("Stopping SampleService");

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
        vars.put("nodes", nodes);
        vars.put("cluster", cluster);
        return vars;
    }

    private void invokeScript(final String type, final ClusterChangedEvent event) {
        client.admin().indices().prepareExists(index)
                .execute(new ActionListener<IndicesExistsResponse>() {
                    @Override
                    public void onResponse(final IndicesExistsResponse response) {
                        if (!response.isExists()) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(index + "/" + type
                                        + " does not exist.");
                            }
                            return;
                        }

                        client.prepareSearch(index).setTypes(type)
                                .setQuery(QueryBuilders.matchAllQuery())
                                .setSize(eventSize)
                                .execute(new ActionListener<SearchResponse>() {
                                    @Override
                                    public void onResponse(
                                            final SearchResponse response) {
                                        final SearchHits hits = response
                                                .getHits();
                                        final long totalHits = hits
                                                .getTotalHits();
                                        if (totalHits == 0) {
                                            if (logger.isDebugEnabled()) {
                                                logger.debug(
                                                        "No scripts for {} event.",
                                                        type);
                                            }
                                            return;
                                        }
                                        if (logger.isDebugEnabled()) {
                                            logger.debug(
                                                    "{}/{} scripts will be executed.",
                                                    hits.getHits().length,
                                                    totalHits);
                                        }
                                        final Map<String, Object> vars = createEventVars();
                                        vars.put("event", event);
                                        for (final SearchHit hit : hits
                                                .getHits()) {
                                            final Map<String, Object> source = hit
                                                    .getSource();
                                            final Object lang = source
                                                    .get("lang");
                                            final Object script = source
                                                    .get("script");
                                            final ScriptType scriptType = getScriptType(source
                                                    .get("script_type"));
                                            if (lang != null && script != null
                                                    && scriptType != null) {
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
                                                            e, lang, script,
                                                            scriptType);
                                                }
                                            }
                                        }
                                    }

                                    @Override
                                    public void onFailure(final Throwable e) {
                                        if (e instanceof ClusterBlockException) {
                                            logger.debug(
                                                    "Cluster is still blocked.",
                                                    e);
                                        } else if (e instanceof SearchPhaseExecutionException) {
                                            logger.debug(
                                                    "{} is not available yet.",
                                                    e, index);
                                        } else {
                                            logger.error(
                                                    "Failed to find scripts for an event hook.",
                                                    e);
                                        }
                                    }
                                });
                    }

                    @Override
                    public void onFailure(final Throwable e) {
                        logger.error("Failed to check if {} exists.", e, index);
                    }
                });

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

        public String setTransientSettings(final String key) {
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
}
