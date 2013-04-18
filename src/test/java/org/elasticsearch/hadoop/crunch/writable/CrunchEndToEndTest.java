/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.crunch.writable;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.hadoop.crunch.ESTarget;
import org.elasticsearch.hadoop.crunch.ESTypedSource;
import org.elasticsearch.hadoop.crunch.ESTarget.Builder;
import org.elasticsearch.hadoop.crunch.writable.domain.UserMessageCount;
import org.elasticsearch.hadoop.util.EmbeddedElasticsearchServer;
import org.elasticsearch.search.SearchHit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * This application reads from ES an existing index of tweets (/twitter/tweet),
 * uses Crunch to count the number of tweets per user and finally writes the
 * result back to ES in a different index type (/twitter/count).
 * 
 * <br/>
 * 
 * Prerequisite: <li>Install Crunch:0.6.0-SNAPSHOT in your local Maven
 * repository.</li>
 * 
 */
public class CrunchEndToEndTest implements Serializable {

  transient private static EmbeddedElasticsearchServer esServer;

  @BeforeClass
  public static void beforeClass() throws ElasticSearchException, IOException {
    esServer = new EmbeddedElasticsearchServer();

    Client client = esServer.getClient();

    // Create new index (twitter) with 3 tweets in the following format:
    // {"user":"user name", "message":"some text"}.
    client.prepareIndex("twitter", "tweet", "1").setSource(createTweet("crunch", "message one")).execute().actionGet();
    client.prepareIndex("twitter", "tweet", "2").setSource(createTweet("crunch", "message two")).execute().actionGet();
    client.prepareIndex("twitter", "tweet", "3").setSource(createTweet("tzolov", "message three")).execute()
        .actionGet();

    // Ensure the new index is searchable.
    esServer.refresIndex("twitter");
  }

  private static XContentBuilder createTweet(String userName, String message) throws IOException {
    return jsonBuilder().startObject().field("user", userName).field("message", message).endObject();
  }

  @AfterClass
  public static void afterClass() {
    esServer.shutdown();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testESSourceAndESTarget() throws InterruptedException {

    assertEquals("Missing test index: 'twitter/tweet'", 3, esServer.countIndex("twitter", "tweet"));

    WritableTypeFamily tf = WritableTypeFamily.getInstance();

    MRPipeline pipeline = new MRPipeline(CrunchEndToEndTest.class);

    // 1. Query all tweets from ES 'twitter' index. The result is represented by
    // collection of java.util.Map elements - one element per ES 'source'
    // object.
    PCollection<Map> tweets = pipeline.read(new ESTypedSource.Builder("twitter/tweet/_search?q=user:*", Map.class)
        .setHost("localhost").setPort(9200).build());

    // 2. Extract the user names from the tweet elements.
    PCollection<String> users = tweets.parallelDo(new MapFn<Map, String>() {
      @Override
      public String map(Map tweet) {
        return tweet.get("user").toString();
      }
    }, tf.strings());

    // 3. Count the number of tweets per user.
    PTable<String, Long> userTweetCount = Aggregate.count(users);

    // 4. Transform the result into JSON serializable format.
    // UserMessageCount defines the JSON format stored in ES
    PCollection<UserMessageCount> esUserTweetCount = userTweetCount.parallelDo(
        new MapFn<Pair<String, Long>, UserMessageCount>() {
          @Override
          public UserMessageCount map(Pair<String, Long> messageCount) {
            return new UserMessageCount(messageCount.first(), messageCount.second());
          }
        }, tf.records(UserMessageCount.class));

    // 5. Write the result into ('twitter/count') ES index type.
    // (http://localhost:9200/twitter/count/_search?q=*)
    pipeline.write(esUserTweetCount, new ESTarget.Builder("twitter/count").setHost("localhost").setPort(9200).build());

    // 6. Execute the pipeline
    assertTrue("Pipeline exectuion has failed!", pipeline.done().succeeded());

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
