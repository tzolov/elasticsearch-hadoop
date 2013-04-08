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

import java.io.Serializable;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

/**
 * Sample application that reads a tweeter index from ES, uses Crunch to count
 * the number of tweets per user and write the result as different ES index
 * type.
 * 
 * Prerequisites:
 * 
 * - Running ElasticSerach node at: http://localhost:9200
 * 
 * - Create sample twitter index as explained here:
 * https://github.com/elasticsearch/elasticsearch#indexing.
 * 
 * Check the resutl index: http://localhost:9200/twitter/tweet/_search?q=*
 * 
 * @author (Christian Tzolov) tzolov@apache.org
 */
public class ESCrunchIntegartionTest implements Serializable {

  // @Test
  public void testESSourceAndESTarget() throws InterruptedException {

    // NOTE: The Avro TypeFamily is not supported at the moment
    WritableTypeFamily tf = WritableTypeFamily.getInstance();

    MRPipeline pipeline = new MRPipeline(ESCrunchIntegartionTest.class);

    // 1. Get all tweets from the 'twitter' ES index. The result is a collection
    // of MapWritable elements - one element per ES 'source' object.
    PCollection<MapWritable> tweets = pipeline.read(new ESSource.Builder("twitter/tweet/_search?q=user:*")
        .setHost("localhost").setPort(9200).build());

    // 2. Get the user names form the input tweets and project out the rest.
    PCollection<String> users = tweets.parallelDo(new MapFn<MapWritable, String>() {
      @Override
      public String map(MapWritable inputMap) {
        return inputMap.get(new Text("user")).toString();
      }
    }, tf.strings());

    // 3. Count the number of tweets per user
    PTable<String, Long> numberOfTweetsPerUser = Aggregate.count(users);

    // 4. Generate ES compatible output format. The UserMessageCountSchema class
    // is used to define the JSON format stored in ES.
    PCollection<UserMessageCountSchema> esUserTweetCount = numberOfTweetsPerUser.parallelDo(
        new MapFn<Pair<String, Long>, UserMessageCountSchema>() {

          @Override
          public UserMessageCountSchema map(Pair<String, Long> userToMessageCount) {

            UserMessageCountSchema esUserMessageCount = new UserMessageCountSchema();

            esUserMessageCount.setUserName(userToMessageCount.first());
            esUserMessageCount.setTweetCount(userToMessageCount.second());

            return esUserMessageCount;
          }
        }, tf.records(UserMessageCountSchema.class));

    // 5. Write the counts into a new ES index type ('twitter/count'). Open the
    // following link to verify the result:
    // http://localhost:9200/twitter/count/_search?q=*
    pipeline.write(esUserTweetCount, new ESTarget.Builder("twitter/count/").setHost("localhost").setPort(9200).build());

    pipeline.done();
  }

  public static void main(String[] args) throws InterruptedException {
    new ESCrunchIntegartionTest().testESSourceAndESTarget();
  }
}
