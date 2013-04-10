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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;

import junit.framework.Assert;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Sample application that reads a tweeter index from ES, uses Crunch to count
 * the number of tweets per user and write the result back to ES in a different
 * index type.
 * 
 * <br/>
 * 
 * Prerequisites:
 * 
 * <li>Install Crunch:0.6.0-SNAPSHOT in your local Maven repository.</li>
 * 
 * <li>Start ElasticSerach node accessible at: http://localhost:9200</li>
 * 
 * @author (Christian Tzolov) tzolov@apache.org
 */
public class ESCrunchIntegartionTest implements Serializable {

  transient private static Node node;
  transient private static Client client;

  @BeforeClass
  public static void before() throws IOException, InterruptedException, ExecutionException {
    node = nodeBuilder().client(false).node();
    client = node.client();

//    client.admin().indices().prepareCreate("twitter").execute().actionGet();
    client.prepareIndex("twitter", "tweet", "1")
        .setSource(jsonBuilder().startObject().field("user", "kimchy").field("message", "hellow1").endObject())
        .execute().actionGet();
    client.prepareIndex("twitter", "tweet", "2")
        .setSource(jsonBuilder().startObject().field("user", "kimchy").field("message", "hellow2").endObject())
        .execute().actionGet();
    client.prepareIndex("twitter", "tweet", "3")
        .setSource(jsonBuilder().startObject().field("user", "tzolov").field("message", "hellow3").endObject())
        .execute().actionGet();
  }

  @AfterClass
  public static void after() {
    client.admin().indices().prepareDelete("twitter").execute().actionGet();
    node.close();
  }

  @Test
  public void testESSourceAndESTarget() throws InterruptedException {

    // NOTE: The AvroTypeFamily is not supported yet.
    WritableTypeFamily tf = WritableTypeFamily.getInstance();

    // Create new Crunch pipeline
    MRPipeline pipeline = new MRPipeline(ESCrunchIntegartionTest.class);

    // 1. Get all tweets from the 'twitter' ES index. The result is a collection
    // of MapWritable elements - one element per ES 'source' object.
    PCollection<MapWritable> tweets = pipeline.read(new ESSource.Builder("twitter/tweet/_search?q=user:*")
        .setHost("localhost").setPort(9200).build());

    // 2. Extract the user names form the tweets.
    PCollection<String> users = tweets.parallelDo(new MapFn<MapWritable, String>() {
      @Override
      public String map(MapWritable inputMap) {
        return inputMap.get(new Text("user")).toString();
      }
    }, tf.strings());

    // 3. Get the number of tweets per user.
    PTable<String, Long> numberOfTweetsPerUser = Aggregate.count(users);

    // 4. Generate ES compatible output format. The UserMessageCountSchema
    // class defines the JSON format stored in ES.
    PCollection<UserMessageCountSchema> esUserTweetCount = numberOfTweetsPerUser.parallelDo(
        new MapFn<Pair<String, Long>, UserMessageCountSchema>() {
          @Override
          public UserMessageCountSchema map(Pair<String, Long> userToMessageCount) {
            return new UserMessageCountSchema(userToMessageCount.first(), userToMessageCount.second());
          }
        }, tf.records(UserMessageCountSchema.class));

    // 5. Write the result into ('twitter/count') ES index type. Check the
    // result: http://localhost:9200/twitter/count/_search?q=*
    pipeline.write(esUserTweetCount, new ESTarget.Builder("twitter/count").setHost("localhost").setPort(9200).build());

    pipeline.done();

    Assert.assertEquals(3, client.prepareCount("twitter").setTypes("tweet").execute().actionGet().count());
    Assert.assertEquals(2, client.prepareCount("twitter").setTypes("count").execute().actionGet().count());
//    System.out.println(client.prepareGet().setIndex("twitter").setType("count").setFields("userName").execute().actionGet());
  }
}