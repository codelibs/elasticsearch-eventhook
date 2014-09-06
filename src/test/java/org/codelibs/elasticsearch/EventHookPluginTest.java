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
        }).build(new String[] { "-numOfNode", "3", "-indexStoreType", "ram" });

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

        String eventIndex = ".eventhook";
        runner.createIndex(eventIndex, null);
        runner.ensureYellow(eventIndex);

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

        // close 1 node
        final Node node1 = runner.node();
        node1.close();
        final Node node2 = runner.node();
        assertTrue(node1 != node2);
        assertTrue(runner.getNode(0).isClosed());
        assertFalse(runner.getNode(1).isClosed());
        assertFalse(runner.getNode(2).isClosed());

    }
}
