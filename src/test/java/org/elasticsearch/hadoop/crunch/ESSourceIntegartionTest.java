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
import org.junit.Test;

/**
 * Note: for this integration test to work you should have a running
 * elasticserach node at localhost:9200. Populate the ES node with the sample
 * data as shown here: https://github.com/elasticsearch/elasticsearch#indexing
 * 
 * @author (Christian Tzolov) tzolov@apache.org
 */
public class ESSourceIntegartionTest implements Serializable {

  @Test
  public void testCrunchElasticSearchIntegration() throws InterruptedException {

    // NOTE: The Avro Type Family is not supported at the moment

    WritableTypeFamily tf = WritableTypeFamily.getInstance();

    MRPipeline pipeline = new MRPipeline(ESSourceIntegartionTest.class);

    // Expects an initialized and populated ES 'twitter' index
    ESSource esSource = new ESSource.Builder("twitter/tweet/_search?q=user:*").setHost("localhost").setPort(9200)
        .build();

    PCollection<MapWritable> tweets = pipeline.read(esSource);

    // Extracts the user names from the ES response
    PCollection<String> users = tweets.parallelDo(new MapFn<MapWritable, String>() {
      @Override
      public String map(MapWritable inputMap) {
        String userName = inputMap.get(new Text("user")).toString();
        return userName;
      }
    }, tf.strings());

    // Count number of tweets per user
    PTable<String, Long> numberOfTweetsPerUser = Aggregate.count(users);

    // Create the output ES compatible output.
    PCollection<UserMessageCountSchema> esUserTweetCount = numberOfTweetsPerUser.parallelDo(
        new MapFn<Pair<String, Long>, UserMessageCountSchema>() {

          @Override
          public UserMessageCountSchema map(Pair<String, Long> userToMessageCount) {

            UserMessageCountSchema esUserMessageCount = new UserMessageCountSchema();
            esUserMessageCount.setUserName(userToMessageCount.first());
            esUserMessageCount.setTweetCount("" + userToMessageCount.second());

            return esUserMessageCount;
          }
        }, tf.records(UserMessageCountSchema.class));

    // Update the index
    pipeline.write(esUserTweetCount, new ESTarget("localhost", 9200, "twitter/count/"));

    pipeline.done();
  }
}
