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
 * This class implements Writable just to be compatible with Crunch's type
 * system. The class is used for JSON serialization only. It is not stored in
 * Hadoop. Therefore it doesn't need readFeildes or write
 * 
 * Relies on Jackson's default Object serialization
 */
public class UserMessageCountSchema implements Writable, Serializable {

  private String userName;

  private long tweetCount;

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
    return Objects.toStringHelper(this).add("user", userName).add("tweet_count", tweetCount).toString();
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
