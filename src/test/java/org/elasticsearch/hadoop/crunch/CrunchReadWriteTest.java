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
import static org.apache.crunch.types.writable.Writables.records;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

/**
 * Prerequisite: <li>Install Crunch:0.6.0-SNAPSHOT in your local Maven
 * repository.</li>
 */
public class CrunchReadWriteTest implements Serializable {

  transient private static EmbeddedElasticsearchServer esServer;

  @BeforeClass
  public static void before() {
    esServer = new EmbeddedElasticsearchServer();
  }

  @AfterClass
  public static void after() {
    esServer.shutdown();
  }

  @Test
  public void testWritesToESAndReadFromES() {

    testWritesToES();

    testReadFromES();
  }

  private void testWritesToES() {

    MRPipeline pipeline = new MRPipeline(CrunchReadWriteTest.class);
    
    PCollection<Artist> artists = pipeline.read(From.textFile("src/test/resources/artists.dat")).parallelDo(
        "Convet input lines into Artist instances", new DoFn<String, Artist>() {

          @Override
          public void process(String line, Emitter<Artist> emitter) {

            String[] fields = line.split("\\t");

            if (fields.length == 4) {
              emitter.emit(new Artist(fields[1], fields[2], fields[3]));
            } else {
              System.out.println("Skip bogus line: " + line);
            }
          }
        }, records(Artist.class));

    pipeline.write(artists, new ESTarget("radio/artists"));

    boolean succeeded = pipeline.done().succeeded();

    assertTrue(succeeded);

    // Refresh the radio index created in testWritesToES() to make it available
    // for searching
    esServer.refresIndex("radio");

    assertEquals(987, esServer.countIndex("radio", "artists"));
  }

  private void testReadFromES() {

    MRPipeline pipeline = new MRPipeline(CrunchReadWriteTest.class);

    Iterable<Artist> artists = pipeline.read(new ESSource("radio/artists/_search?q=me*"))
        .parallelDo(new MapFn<MapWritable, Artist>() {
          @Override
          public Artist map(MapWritable input) {
            String name = input.get(new Text("name")).toString();
            String url = input.get(new Text("url")).toString();
            String picture = input.get(new Text("picture")).toString();
            return new Artist(name, url, picture);
          }
        }, records(Artist.class)).materialize();

    assertEquals(15, Lists.newArrayList(artists).size());
  }
}
