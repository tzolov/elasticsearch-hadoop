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
import static org.apache.crunch.types.writable.Writables.records;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

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
public class CrunchHadoopTest implements Serializable {

  static class Artist implements Writable, Serializable {
    private String name;
    private String url;
    private String picture;

    public Artist(String name, String url, String picture) {
      this.name = name;
      this.url = url;
      this.picture = picture;
    }

    public Artist() {
    }

    public String getName() {
      return name;
    }

    public String getUrl() {
      return url;
    }

    public String getPicture() {
      return picture;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public void setPicture(String picture) {
      this.picture = picture;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      name = in.readUTF();
      url = in.readUTF();
      picture = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(name);
      out.writeUTF(url);
      out.writeUTF(picture);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(Artist.class).add("name", name).add("url", url).add("pic", picture).toString();
    }
  }

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
    
    // Refresh the radio index created in testWritesToES() to make it available
    // for searching
    esServer.refresIndex("radio");
    
    testReadFromES();
  }

  private void testWritesToES() {

    MRPipeline pipeline = new MRPipeline(CrunchHadoopTest.class);

    PCollection<Artist> artists = pipeline.read(From.textFile("src/test/resources/artists.dat")).parallelDo(
        new DoFn<String, Artist>() {

          @Override
          public void process(String line, Emitter<Artist> emitter) {
            String[] fields = line.split("\\t");
            if (fields.length == 4) {
              emitter.emit(new Artist(fields[1], fields[2], fields[3]));
            } else {
              System.out.println("bogus line:" + line);
            }
          }
        }, records(Artist.class));

    pipeline.write(artists, new ESTarget.Builder("radio/artists").build());

    pipeline.done();
  }

  private void testReadFromES() {

    MRPipeline pipeline = new MRPipeline(CrunchHadoopTest.class);

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

    ArrayList<Artist> artistList = Lists.newArrayList(artists);

    assertEquals(15, artistList.size());

    pipeline.done();
  }
}
