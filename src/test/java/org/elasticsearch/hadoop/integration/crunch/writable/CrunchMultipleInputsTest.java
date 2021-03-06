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
package org.elasticsearch.hadoop.integration.crunch.writable;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.hadoop.crunch.ESSource;
import org.elasticsearch.hadoop.integration.LocalES;
import org.elasticsearch.hadoop.integration.crunch.ESTestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.Lists;

@SuppressWarnings("rawtypes")
public class CrunchMultipleInputsTest implements Serializable {

  @ClassRule
  public static LocalES esServer = new LocalES();

  @BeforeClass
  public static void beforeClass() throws ElasticSearchException, IOException {

    Client client = esServer.getClient();

    client.prepareIndex("twitter1", "tweet", "1").setSource(ESTestUtil.createTweet("crunch", "message one")).execute()
        .actionGet();
    client.prepareIndex("twitter2", "tweet", "1").setSource(ESTestUtil.createTweet("crunch", "message two")).execute()
        .actionGet();

    esServer.refresIndex("twitter1");
    esServer.refresIndex("twitter2");
  }

  static class ExtractField extends MapFn<Map, String> {
    private String field;

    public ExtractField(String field) {
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

    PCollection<String> users1 = pipeline.read(
        new ESSource.Builder<Map>("twitter1/tweet/_search?q=*", Map.class).setPort(9500).build()).parallelDo(
        new ExtractField("user"), tf.strings());

    PCollection<String> users2 = pipeline.read(
        new ESSource.Builder<Map>("twitter2/tweet/_search?q=*", Map.class).setPort(9500).build()).parallelDo(
        new ExtractField("user"), tf.strings());

//    for (String s: Lists.newArrayList(users1.materialize().iterator())) {
//      System.out.println(s);
//    }
    assertEquals(2, Lists.newArrayList(users1.union(users2).materialize().iterator()).size());
  }
}
