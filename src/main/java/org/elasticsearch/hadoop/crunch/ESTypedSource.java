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

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.crunch.Source;
import org.apache.crunch.io.CrunchInputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.ESInputFormat;

import com.google.common.base.Objects;

public class ESTypedSource<T> implements Source<T> {

  private PType<T> ptype;
  private String esQuery;

  private String host = "localhost"; 
  private int port = 9200;

  public ESTypedSource(String esQuery, Class<T> classType) {
    this.ptype = ESTypes.jsonMapWritable(classType, WritableTypeFamily.getInstance());
    this.esQuery = esQuery;
  }

  public static class Builder<T> {

    private ESTypedSource<T> esSource;

    public Builder(String esQuery, Class<T> classType) {
      esSource = new ESTypedSource<T>(esQuery, classType);
    }

    public Builder<T> setHost(String host) {
      esSource.host = host;
      return this;
    }

    public Builder<T> setPort(int port) {
      esSource.port = port;
      return this;
    }

    public ESTypedSource<T> build() {
      return esSource;
    }
  }

  @Override
  public PType<T> getType() {
    return ptype;
  }

  @Override
  public void configureSource(Job job, int inputId) throws IOException {

    if (inputId == -1) {// single input

      Configuration conf = job.getConfiguration();

      conf.set(ConfigurationOptions.ES_HOST, host);
      conf.set(ConfigurationOptions.ES_PORT, "" + port);
      conf.set(ConfigurationOptions.ES_QUERY, esQuery);

      job.setInputFormatClass(ESInputFormat.class);

    } else { // multiple inputs

      FormatBundle<ESInputFormat> inputBundle = FormatBundle.forInput(ESInputFormat.class)
          .set(ConfigurationOptions.ES_HOST, host).set(ConfigurationOptions.ES_PORT, "" + port)
          .set(ConfigurationOptions.ES_QUERY, esQuery);

      Path dummy = new Path("/es/" + Base64.encodeBase64String(esQuery.getBytes()));

      CrunchInputs.addInputPath(job, dummy, inputBundle, inputId);
    }
  }

  @Override
  public long getSize(Configuration configuration) {
    // TODO Do something smarter here. Use the ES result metadata???
    return 1000 * 1000;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(esQuery, host, port);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ESTypedSource other = (ESTypedSource) obj;

    return Objects.equal(esQuery, other.esQuery) && Objects.equal(host, other.host) && Objects.equal(port, other.port);
  }
}
