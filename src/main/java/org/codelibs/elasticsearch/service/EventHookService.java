package org.codelibs.elasticsearch.service;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
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
            final Map<String, Object> vars = new HashMap<String, Object>();
            vars.put("event", event);
            vars.put("isMaster", isMaster);
            invokeScript(type, vars);
        }
    }

    private void invokeScript(final String type, final Map<String, Object> vars) {
        client.admin().indices().prepareExists(index)
                .execute(new ActionListener<IndicesExistsResponse>() {
                    @Override
                    public void onResponse(IndicesExistsResponse response) {
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
                                        SearchHits hits = response.getHits();
                                        long totalHits = hits.getTotalHits();
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
                                        for (SearchHit hit : hits.getHits()) {
                                            Map<String, Object> source = hit
                                                    .getSource();
                                            Object lang = source.get("lang");
                                            Object script = source
                                                    .get("script");
                                            ScriptType scriptType = getScriptType(source
                                                    .get("scriptType"));
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
                                                } catch (Exception e) {
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
                    public void onFailure(Throwable e) {
                        logger.error("Failed to check if {} exists.", e, index);
                    }
                });

    }

    protected String getEventType(String source) {
        if (source != null && source.length() > 0) {
            return source.replaceAll("[\\(\\[].*", "").trim()
                    .replaceAll("[\\s\\-]", "_");
        }
        return EVENT_TYPE_UNKNOWN;
    }

    private static ScriptType getScriptType(Object scriptType) {
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
    }

    @Override
    public void offMaster() {
        isMaster = false;
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.MANAGEMENT;
    }

}
