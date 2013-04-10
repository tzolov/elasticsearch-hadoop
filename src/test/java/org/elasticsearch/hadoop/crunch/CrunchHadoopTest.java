package org.elasticsearch.hadoop.crunch;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import junit.framework.Assert;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

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

  transient private static Node node;
  transient private static Client client;

  @BeforeClass
  public static void before() {
    node = nodeBuilder().client(false).node();
    client = node.client();
  }

  @AfterClass
  public static void after() {
    client.prepareDelete().setIndex("radio").execute();
    node.close();
  }

  @Test
  public void testWriteES() {

    MRPipeline pipeline = new MRPipeline(CrunchHadoopTest.class);

    PCollection<Artist> artists = pipeline.read(From.textFile("src/test/resources/artists.dat")).parallelDo(
        new DoFn<String, Artist>() {

          @Override
          public void process(String line, Emitter<Artist> emitter) {
            String[] fields = line.split("\\t");
            if (fields.length == 4) {
              emitter.emit(new Artist(fields[1], fields[2], fields[3]));
            } else {
              System.out.println("Bogus line:" + line);
            }

          }
        }, Writables.records(Artist.class));

    pipeline.write(artists, new ESTarget.Builder("radio/artists").build());

    pipeline.done();
  }

  @Test
  public void testReadES() {

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
        }, Writables.records(Artist.class)).materialize();

    ArrayList<Artist> artistList = Lists.newArrayList(artists);

    Assert.assertEquals(15, artistList.size());

    pipeline.done();
  }
}
