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
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.hadoop.crunch.ESTypedSource;
import org.elasticsearch.hadoop.util.EmbeddedElasticsearchServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Prerequisite: <li>Install Crunch:0.6.0-SNAPSHOT in your local Maven
 * repository.</li>
 * 
 */
public class CrunchMultipleInputsTest implements Serializable {

  transient private static EmbeddedElasticsearchServer esServer;

  @BeforeClass
  public static void beforeClass() throws ElasticSearchException, IOException {
    esServer = new EmbeddedElasticsearchServer();

    Client client = esServer.getClient();

    client.prepareIndex("twitter1", "tweet", "1").setSource(createTweet("crunch", "message one")).execute().actionGet();
    client.prepareIndex("twitter2", "tweet", "1").setSource(createTweet("crunch", "message two")).execute().actionGet();

    esServer.refresIndex("twitter1");
    esServer.refresIndex("twitter2");
  }

  private static XContentBuilder createTweet(String userName, String message) throws IOException {
    return jsonBuilder().startObject().field("user", userName).field("message", message).endObject();
  }

  @AfterClass
  public static void afterClass() {
    esServer.shutdown();
  }

  static class ExtractMapField extends MapFn<Map, String> {
    private String field;

    public ExtractMapField(String field) {
      this.field = field;
    }

    @Override
    public String map(Map inputMap) {
      return inputMap.get(field).toString();
    }
  }

  @Test
  public void testReadMultipleESInputs() throws InterruptedException {

    // Ensure the test index is initialized
    assertEquals(1, esServer.countIndex("twitter1", "tweet"));
    assertEquals(1, esServer.countIndex("twitter2", "tweet"));

    WritableTypeFamily tf = WritableTypeFamily.getInstance();

    // Create new Crunch pipeline
    MRPipeline pipeline = new MRPipeline(CrunchMultipleInputsTest.class);

    PCollection<String> users1 = pipeline.read(new ESTypedSource<Map>("twitter1/tweet/_search?q=*", Map.class))
        .parallelDo(new ExtractMapField("user"), tf.strings());

    PCollection<String> users2 = pipeline.read(new ESTypedSource<Map>("twitter2/tweet/_search?q=*", Map.class))
        .parallelDo(new ExtractMapField("user"), tf.strings());

    assertEquals(2, Lists.newArrayList(users1.union(users2).materialize().iterator()).size());
  }
}
