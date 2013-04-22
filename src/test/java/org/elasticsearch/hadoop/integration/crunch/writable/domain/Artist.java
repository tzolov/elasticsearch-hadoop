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
package org.elasticsearch.hadoop.integration.crunch.writable.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Objects;

public class Artist implements Writable, Serializable {
  
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