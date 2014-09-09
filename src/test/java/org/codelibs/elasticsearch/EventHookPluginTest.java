package org.codelibs.elasticsearch;

import junit.framework.TestCase;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.sort.SortBuilders;

public class EventHookPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    @Override
    protected void setUp() throws Exception {
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
            }
        }).build(new String[] { "-numOfNode", "5", "-indexStoreType", "ram" });

        // wait for yellow status
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_runCluster() throws Exception {

        final String eventIndex = ".eventhook";
        runner.createIndex(eventIndex, null);
        runner.ensureYellow(eventIndex);

        runner.insert(
                eventIndex,
                "all",
                "print_event",
                "{\"priority\":1,\"lang\":\"groovy\","
                        + "\"script\":\"println(\\\"EVENT[\\\"+cluster.getLocalNode().name()+\\\"]:\\\"+eventType+\\\" => \\\"+event.source())\","
                        + "\"script_type\":\"inline\"}");
        runner.insert(
                eventIndex,
                "routing_table_updater",
                "allocation_disable",
                "{\"priority\":2,\"lang\":\"groovy\","
                        + "\"script\":\"if(nodes.nodeInfo().length==4){"
                        + "cluster.setTransientSettings(\\\"cluster.routing.allocation.enable\\\",\\\"none\\\");"
                        + "println(\\\"EVENT[\\\"+cluster.getLocalNode().name()+\\\"]: allocation disabled\\\")}\","
                        + "\"script_type\":\"inline\"}");
        runner.insert(
                eventIndex,
                "zen_disco_node_left",
                "allocation_enable",
                "{\"priority\":2,\"lang\":\"groovy\","
                        + "\"script\":\"if(nodes.nodeInfo().length==3){"
                        + "cluster.setTransientSettings(\\\"cluster.routing.allocation.enable\\\",\\\"all\\\");"
                        + "println(\\\"EVENT[\\\"+cluster.getLocalNode().name()+\\\"]: allocation enabled\\\")}\","
                        + "\"script_type\":\"inline\"}");

        final String index = "test_index";
        final String type = "test_type";

        // create an index
        runner.createIndex(index, null);
        runner.ensureYellow(index);

        // create a mapping
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                .startObject()//
                .startObject(type)//
                .startObject("properties")//

                // id
                .startObject("id")//
                .field("type", "string")//
                .field("index", "not_analyzed")//
                .endObject()//
                // msg
                .startObject("msg")//
                .field("type", "string")//
                .endObject()//
                .endObject()//
                .endObject()//
                .endObject();
        runner.createMapping(index, type, mappingBuilder);

        if (!runner.indexExists(index)) {
            fail();
        }

        // create 1000 documents
        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, type,
                    String.valueOf(i), "{\"id\":\"" + i + "\",\"msg\":\"test "
                            + i + "\"}");
            assertTrue(indexResponse1.isCreated());
        }
        runner.refresh();

        // search 1000 documents
        {
            final SearchResponse searchResponse = runner.search(index, type,
                    null, null, 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
            assertEquals(10, searchResponse.getHits().hits().length);
        }

        {
            final SearchResponse searchResponse = runner.search(index, type,
                    QueryBuilders.matchAllQuery(),
                    SortBuilders.fieldSort("id"), 0, 10);
            assertEquals(1000, searchResponse.getHits().getTotalHits());
            assertEquals(10, searchResponse.getHits().hits().length);
        }

        // delete 1 document
        runner.delete(index, type, String.valueOf(1));
        runner.flush();

        {
            final SearchResponse searchResponse = runner.search(index, type,
                    null, null, 0, 10);
            assertEquals(999, searchResponse.getHits().getTotalHits());
            assertEquals(10, searchResponse.getHits().hits().length);
        }

        // optimize
        runner.optimize(false);

        // close master node
        final Node masterNode = runner.masterNode();
        masterNode.close();

        // wait
        Thread.sleep(5000L);

        assertEquals("none", runner.clusterService().state().metaData()
                .transientSettings().get("cluster.routing.allocation.enable"));

        // close master node
        final Node nonMasterNode = runner.nonMasterNode();
        nonMasterNode.close();

        // wait
        Thread.sleep(5000L);

        assertEquals("all", runner.clusterService().state().metaData()
                .transientSettings().get("cluster.routing.allocation.enable"));

    }
}
