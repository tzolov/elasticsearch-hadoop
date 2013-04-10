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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Objects;

/**
 * This class defines the JSON format to be stored in ES. Jackson's ObjectMapper
 * (inside the RestClient) converts the output Crunch data into JSON source
 * objects stored in ES.
 * 
 * Note: UserMessageCountSchema implements the Writable interface to fit with
 * Crunch's WritableTypeFamily. But because it is not used for any Hadoop
 * storage (only as meant to encode JSON in ES) the Writable methods are empty.
 */
public class UserMessageCountSchema implements Writable, Serializable {

  private String userName;

  private long tweetCount;

  public UserMessageCountSchema() {
    this.userName = null;
    this.tweetCount = -1;
  }

  public UserMessageCountSchema(String userName, long tweetCount) {
    this.userName = userName;
    this.tweetCount = tweetCount;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public long getTweetCount() {
    return tweetCount;
  }

  public void setTweetCount(long count) {
    this.tweetCount = count;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("user", userName).add("tweetCount", tweetCount).toString();
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    // Not used for the ES
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    // Not used for the ES
  }
}
