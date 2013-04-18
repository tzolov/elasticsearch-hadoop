package org.elasticsearch.hadoop.integration.crunch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.hadoop.integration.LocalES;
import org.elasticsearch.search.SearchHit;

import com.google.common.collect.Sets;

public class ESTestUtil {

  public static void populateESWithTestTwittData(LocalES esServer) throws ElasticSearchException,
      IOException {

    Client client = esServer.getClient();

    // Create new index (twitter) with 3 tweets in the following format:
    // {"user":"user name", "message":"some text"}.
    client.prepareIndex("twitter", "tweet", "1").setSource(createTweet("crunch", "message one")).execute().actionGet();
    client.prepareIndex("twitter", "tweet", "2").setSource(createTweet("crunch", "message two")).execute().actionGet();
    client.prepareIndex("twitter", "tweet", "3").setSource(createTweet("tzolov", "message three")).execute()
        .actionGet();

    // Ensure the new index is searchable.
    esServer.refresIndex("twitter");

    assertEquals("Missing test index: 'twitter/tweet'", 3, esServer.countIndex("twitter", "tweet"));
  }

  public static XContentBuilder createTweet(String userName, String message) throws IOException {
    return jsonBuilder().startObject().field("user", userName).field("message", message).endObject();
  }

  public static void checkResultTwitterIndex(LocalES esServer) {

    // Refresh the 'twitter' index to ensure it is available for querying.
    esServer.refresIndex("twitter");

    assertEquals(3, esServer.countIndex("twitter", "tweet"));
    assertEquals("Result index 'twitter/count' should contain to entries", 2, esServer.countIndex("twitter", "count"));

    HashSet<SearchHit> resultCountIndex = Sets.newHashSet(esServer.searchIndex("twitter", "count"));
    assertEquals("Result should contain 2 hits!", 2, resultCountIndex.size());

    HashSet<String> expecteCountIndex = Sets.newHashSet("{\"userName\":\"tzolov\",\"tweetCount\":1}",
        "{\"userName\":\"crunch\",\"tweetCount\":2}");

    for (SearchHit hit : resultCountIndex) {
      assertTrue(expecteCountIndex.contains(hit.getSourceAsString()));
    }
  }
}
