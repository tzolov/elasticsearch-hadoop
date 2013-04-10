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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.io.FileUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Prerequisite: <li>Install Crunch:0.6.0-SNAPSHOT in your local Maven
 * repository.</li>
 */
public class CrunchMultipleOutputsTest implements Serializable {

  transient private static EmbeddedElasticsearchServer esServer;

  @BeforeClass
  public static void before() {
    esServer = new EmbeddedElasticsearchServer();
  }

  @AfterClass
  public static void after() {
    esServer.shutdown();
  }

  static class ConvertStringToArtist extends DoFn<String, Artist> {
    @Override
    public void process(String line, Emitter<Artist> emitter) {

      String[] fields = line.split("\\t");

      if (fields.length == 4) {
        emitter.emit(new Artist(fields[1], fields[2], fields[3]));
      } else {
        System.out.println("Skip bogus line: " + line);
      }
    }
  }

  @Test
  public void testWriteToMultipleESIndexes() throws IOException {

    File firstInputFile = new File("src/test/resources/artists.dat");
    File secondInputFile = File.createTempFile("artists", ".dat");

    FileUtils.copyFile(firstInputFile, secondInputFile);
    secondInputFile.deleteOnExit();

    MRPipeline pipeline = new MRPipeline(CrunchMultipleOutputsTest.class);

    PCollection<Artist> artists1 = pipeline.read(From.textFile(firstInputFile.getAbsolutePath())).parallelDo(
        "Convet input lines into Artist instances - 1", new ConvertStringToArtist(), records(Artist.class));

    PCollection<Artist> artists2 = pipeline.read(From.textFile(secondInputFile.getAbsolutePath())).parallelDo(
        "Convet input lines into Artist instances - 2", new ConvertStringToArtist(), records(Artist.class));

    PCollection<Artist> artistsUnion = artists1.union(artists2);

    pipeline.write(artistsUnion, new ESTarget("radio/artists"));
    pipeline.write(artists1, new ESTarget("radio/artists1"));
    pipeline.write(artists2, new ESTarget("radio/artists2"));

    boolean succeeded = pipeline.done().succeeded();

    assertTrue(succeeded);

    // Refresh the radio index created in testWritesToES() to make it available
    // for searching
    esServer.refresIndex("radio");

    assertEquals(2 * 987, esServer.countIndex("radio", "artists"));
    assertEquals(987, esServer.countIndex("radio", "artists1"));
    assertEquals(987, esServer.countIndex("radio", "artists2"));
  }
}
