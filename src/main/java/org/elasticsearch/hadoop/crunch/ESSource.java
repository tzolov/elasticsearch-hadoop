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
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.mr.ESConfigConstants;
import org.elasticsearch.hadoop.mr.ESInputFormat;
import org.elasticsearch.hadoop.util.ConfigUtils;

import com.google.common.base.Objects;

public class ESSource implements Source<MapWritable> {

  private PType<MapWritable> ptype;
  private String esQuery;

  private String host = null;
  private int port = -1;

  public ESSource(String esQuery) {
    this.ptype = Writables.writables(MapWritable.class);
    this.esQuery = esQuery;
  }

  static class Builder {

    private ESSource esSource;

    public Builder(String esQuery) {
      esSource = new ESSource(esQuery);
    }

    public Builder setHost(String host) {
      esSource.host = host;
      return this;
    }

    public Builder setPort(int port) {
      esSource.port = port;
      return this;
    }

    public ESSource build() {
      return esSource;
    }
  }

  @Override
  public PType<MapWritable> getType() {
    return ptype;
  }

  @Override
  public void configureSource(Job job, int inputId) throws IOException {

    Configuration conf = job.getConfiguration();

    if (inputId == -1) {// single input

      conf.set(ESConfigConstants.ES_ADDRESS, ConfigUtils.detectHostPortAddress(host, port, conf));
      conf.set(ESConfigConstants.ES_QUERY, esQuery);
      conf.set(ESConfigConstants.ES_LOCATION, esQuery);
      job.setInputFormatClass(ESInputFormat.class);

    } else { // multiple inputs
      
      FormatBundle<ESInputFormat> inputBundle = FormatBundle.forInput(ESInputFormat.class)
          .set(ESConfigConstants.ES_ADDRESS, ConfigUtils.detectHostPortAddress(host, port, conf))
          .set(ESConfigConstants.ES_QUERY, esQuery).set(ESConfigConstants.ES_LOCATION, esQuery);

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
    ESSource other = (ESSource) obj;

    return Objects.equal(esQuery, other.esQuery) && Objects.equal(host, other.host) && Objects.equal(port, other.port);
  }
}
