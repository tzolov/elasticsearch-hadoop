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
package org.elasticsearch.hadoop.integration.crunch.writable.e2e;

import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.hadoop.crunch.ESSource;
import org.elasticsearch.hadoop.crunch.ESTarget;
import org.elasticsearch.hadoop.integration.LocalES;
import org.elasticsearch.hadoop.integration.crunch.ESTestUtil;
import org.elasticsearch.hadoop.integration.crunch.writable.domain.UserMessageCount;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.base.Objects;

/**
 * Reads all tweets from ES (/twitter/tweet) index, represent the input as
 * {@link Tweet} (custom {@link Writable}) entries, uses Crunch to count the
 * number of tweets per user, serialize the result as {@link UserMessageCount}
 * (another {@link Writable}) and writes it back to ES (/twitter/count) index.
 * 
 * <br/>
 * 
 * Prerequisite: <li>Install Crunch:0.6.0-SNAPSHOT in your local Maven
 * repository.</li>
 * 
 */
public class CrunchWritableSerDeIT implements Serializable {

  @ClassRule
  public static LocalES esServer = new LocalES();

  @BeforeClass
  public static void beforeClass() throws ElasticSearchException, IOException {
    ESTestUtil.populateESWithTestTwittData(esServer);
  }

  public static class Tweet implements WritableComparable<Tweet> {
    public String user;
    public String message;

    public String getUser() {
      return user;
    }

    public void setUser(String user) {
      this.user = user;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(user, message);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Tweet other = (Tweet) obj;
      return Objects.equal(user, other.user) && Objects.equal(message, other.message);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      user = in.readUTF();
      message = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(user);
      out.writeUTF(message);
    }

    @Override
    public int compareTo(Tweet o) {
      if (o == null) {
        return 1;
      }
      return user.compareTo(o.user);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("user", user).add("message", message).toString();
    }
  }

  @Test
  public void testMapWritableSerDeser() throws InterruptedException {

    MRPipeline pipeline = new MRPipeline(CrunchWritableSerDeIT.class);

    pipeline.enableDebug();

    // 1. Query all tweets from the 'twitter' index. Result is represented
    // by collection of UserMessageCount. One UserMessageCount instance
    // represents one ES 'source' object.
    PCollection<Tweet> tweets = pipeline.read(new ESSource.Builder<Tweet>("twitter/tweet/_search?q=user:*",
        Tweet.class).setPort(9700).build());

    // TODO find better way to convert the PType
    IdentityFn<Tweet> identityFn = IdentityFn.getInstance();
    tweets = tweets.parallelDo(identityFn, Writables.records(Tweet.class));

    // 2. Count the number of tweets per user. Depends on Tweet's
    // hasCode/equals implementation.
    PTable<Tweet, Long> userTweetCount = Aggregate.count(tweets);

    System.out.println(Lists.newArrayList(userTweetCount.materialize()));
    // 3. Transform the result into JSON serializable format. Here
    // UserMessageCount writable is used to define the JSON format.
    PCollection<UserMessageCount> esUserTweetCount = userTweetCount.parallelDo(
        new MapFn<Pair<Tweet, Long>, UserMessageCount>() {
          @Override
          public UserMessageCount map(Pair<Tweet, Long> messageCount) {
            return new UserMessageCount(messageCount.first().user, messageCount.second());
          }
        }, Writables.records(UserMessageCount.class));

    // 5. Write the result into ('twitter/count') ES index type.
    // (http://localhost:9200/twitter/count/_search?q=*)
    pipeline.write(esUserTweetCount, new ESTarget.Builder("twitter/count").setHost("localhost").setPort(9700).build());

    // 6. Execute the pipeline
    assertTrue("Pipeline exectuion has failed!", pipeline.done().succeeded());

    ESTestUtil.checkResultTwitterIndex(esServer);
  }
}
