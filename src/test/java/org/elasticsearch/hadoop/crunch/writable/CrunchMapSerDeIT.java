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

import static junit.framework.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.types.writable.Writables;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.hadoop.crunch.ESTarget;
import org.elasticsearch.hadoop.crunch.ESTypedSource;
import org.elasticsearch.hadoop.crunch.ESTypes;
import org.elasticsearch.hadoop.util.EmbeddedElasticsearchServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

/**
 * Reads all tweets from ES (/twitter/tweet) index, represent the input as
 * {@link Map} entries, apply Crunch to count the number of tweets per user,
 * serialize the result as {@link Map} and writes it back to ES (/twitter/count)
 * index.
 * 
 * <br/>
 * 
 * Prerequisite: <li>Install Crunch:0.6.0-SNAPSHOT in your local Maven
 * repository.</li>
 * 
 */
public class CrunchMapSerDeIT implements Serializable {

  transient private static EmbeddedElasticsearchServer esServer;

  @BeforeClass
  public static void beforeClass() throws ElasticSearchException, IOException {

    esServer = new EmbeddedElasticsearchServer();

    ESTestUtil.populateESWithTestTwittData(esServer);
  }

  @AfterClass
  public static void afterClass() {
    esServer.shutdown();
  }

  @Test
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void testMapWritableSerDeser() throws InterruptedException {

    MRPipeline pipeline = new MRPipeline(CrunchMapSerDeIT.class);

    // 1. Query all tweets from the 'twitter' index. Result is represented
    // by collection of java.util.Map. One Map element represents one ES
    // 'source' object.
    PCollection<Map> tweets = pipeline.read(new ESTypedSource.Builder("twitter/tweet/_search?q=user:*", Map.class)
        .setHost("localhost").setPort(9200).build());

    // 2. Extract the user names from the tweet elements.
    PCollection<String> users = tweets.parallelDo(new MapFn<Map, String>() {
      @Override
      public String map(Map tweet) {
        return tweet.get("user").toString();
      }
    }, Writables.strings());

    // 3. Count the number of tweets per user.
    PTable<String, Long> userTweetCount = Aggregate.count(users);

    // 4. Transform the result into JSON serializable format. Here java.util.Map
    // is used to define the JSON format.
    PCollection<Map> esUserTweetCount = userTweetCount.parallelDo(new MapFn<Pair<String, Long>, Map>() {

      private Map map = Maps.newHashMap();

      @Override
      public Map map(Pair<String, Long> messageCount) {
        map.clear();
        map.put("userName", messageCount.first());
        map.put("tweetCount", messageCount.second());
        return map;
      }
    }, ESTypes.map());

    // 5. Write the result into ('twitter/count') ES index type.
    // (http://localhost:9200/twitter/count/_search?q=*)
    pipeline.write(esUserTweetCount, new ESTarget.Builder("twitter/count").setHost("localhost").setPort(9200).build());

    // 6. Execute the pipeline
    assertTrue("Pipeline exectuion has failed!", pipeline.done().succeeded());

    ESTestUtil.checkResultTwitterIndex(esServer);
  }
}
