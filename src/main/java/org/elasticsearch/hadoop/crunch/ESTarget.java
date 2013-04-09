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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.MapReduceTarget;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.mr.ESOutputFormat;

import com.google.common.base.Objects;

public class ESTarget implements MapReduceTarget {

  private static final Log LOG = LogFactory.getLog(ESTarget.class);

  private String index;
  private String host = null;
  private int port = -1 ;

  public ESTarget(String index) {
    this.index = index;
  }

  static class Builder {
    
    private ESTarget esTarget;

    public Builder(String index) {
      esTarget = new ESTarget(index);
    }
    
    public Builder setHost(String host) {
      esTarget.host = host;
      return this;
    }
    
    public Builder setPort(int port) {
      esTarget.port = port;
      return this;
    }
    
    public ESTarget build() {
      return esTarget;
    }
  }

  @Override
  public void handleExisting(WriteMode writeMode, Configuration conf) {
    LOG.info("ESTarget ignores checks for existing outputs...");
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    handler.configure(this, ptype);
    return true;
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> ptype) {
    return null;
  }

  @Override
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {

    FileOutputFormat.setOutputPath(job, outputPath);

    Configuration conf = job.getConfiguration();
    SettingsManager.loadFrom(conf).setHost(host).setPort(port).setResource(index).save();

    if (name == null) {
      job.setOutputFormatClass(ESOutputFormat.class);
      job.setOutputValueClass(ptype.getTypeClass());
    } else {
      FormatBundle<ESOutputFormat> bundle = FormatBundle.forOutput(ESOutputFormat.class);
      CrunchOutputs.addNamedOutput(job, name, bundle, String.class, ptype.getTypeClass());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(index, host, port);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ESTarget other = (ESTarget) obj;

    return Objects.equal(index, other.index) && Objects.equal(host, other.host) && Objects.equal(port, other.port);
  }
}
