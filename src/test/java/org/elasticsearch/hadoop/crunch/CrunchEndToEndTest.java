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
package org.elasticsearch.hadoop.crunch;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
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

    // Create 3 tweets in the following format: {"user":"user name", "message":
    // "some text"}.
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

  @Test
  public void testESSourceAndESTarget() throws InterruptedException {

    // Ensure the test index is initialized
    assertEquals("Missing test index: 'twitter/tweet'", 3,
        esServer.getClient().prepareCount("twitter").setTypes("tweet").execute().actionGet().count());

    // NOTE: The AvroTypeFamily is not supported yet.
    WritableTypeFamily tf = WritableTypeFamily.getInstance();

    // Create new Crunch pipeline
    MRPipeline pipeline = new MRPipeline(CrunchEndToEndTest.class);

    // 1. Read all tweets from the 'twitter' ES index. The result is a
    // collection of MapWritable elements - one element per ES 'source' object.
    PCollection<MapWritable> tweets = pipeline.read(new ESSource.Builder("twitter/tweet/_search?q=user:*")
        .setHost("localhost").setPort(9200).build());

    // 2. Extract the user names form the tweets.
    PCollection<String> users = tweets.parallelDo(new MapFn<MapWritable, String>() {
      @Override
      public String map(MapWritable inputMap) {
        return inputMap.get(new Text("user")).toString();
      }
    }, tf.strings());

    // 3. Get the number of tweets per user: <user, msg count>
    PTable<String, Long> numberOfTweetsPerUser = Aggregate.count(users);

    // 4. Convert the output into a format that can be serialized by ES into
    // JSON format. Use a custom UserMessageCountSchema class to define the JSON
    // format to be stored in ES.
    PCollection<UserMessageCountSchema> esUserTweetCount = numberOfTweetsPerUser.parallelDo(
        new MapFn<Pair<String, Long>, UserMessageCountSchema>() {
          @Override
          public UserMessageCountSchema map(Pair<String, Long> userToMessageCount) {
            return new UserMessageCountSchema(userToMessageCount.first(), userToMessageCount.second());
          }
        }, tf.records(UserMessageCountSchema.class));

    // 5. Write the result into ('twitter/count') ES index type.
    // (http://localhost:9200/twitter/count/_search?q=*)
    pipeline.write(esUserTweetCount, new ESTarget.Builder("twitter/count").setHost("localhost").setPort(9200).build());

    // 6. Execute the pipeline
    boolean succeeded = pipeline.done().succeeded();

    assertTrue("Pipeline exectuion has failed!", succeeded);

    // Refresh the 'twitter' index to ensure it is available for querying.
    esServer.refresIndex("twitter");

    assertEquals(3, esServer.getClient().prepareCount("twitter").setTypes("tweet").execute().actionGet().count());
    assertEquals("Missing result index: 'twitter/cont'", 2,
        esServer.getClient().prepareCount("twitter").setTypes("count").execute().actionGet().count());

    HashSet<SearchHit> resultCountIndex = Sets.newHashSet(esServer.getClient().prepareSearch("twitter")
        .setTypes("count").execute().actionGet().getHits().iterator());

    assertEquals("Result should contain 2 hits!", 2, resultCountIndex.size());
    
    HashSet<String> expecteCountIndex = Sets.newHashSet("{\"userName\":\"tzolov\",\"tweetCount\":1}",
        "{\"userName\":\"crunch\",\"tweetCount\":2}");


    for (SearchHit hit : resultCountIndex) {
      assertTrue(expecteCountIndex.contains(hit.getSourceAsString()));
    }
  }
}
