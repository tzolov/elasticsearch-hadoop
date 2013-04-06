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
import org.elasticsearch.hadoop.mr.ESConfigConstants;
import org.elasticsearch.hadoop.mr.ESOutputFormat;
import org.elasticsearch.hadoop.util.ConfigUtils;

import com.google.common.base.Objects;

public class ESTarget implements MapReduceTarget {

  private static final Log LOG = LogFactory.getLog(ESTarget.class);

  private String index;
  private String host;
  private int port;

  public ESTarget(String host, int port, String index) {
    this.host = host;
    this.port = port;
    this.index = index;
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
    conf.set(ESConfigConstants.ES_ADDRESS, ConfigUtils.detectHostPortAddress(host, port, conf));
//    conf.set(ESConfigConstants.ES_QUERY, index.trim());
    conf.set(ESConfigConstants.ES_INDEX, index.trim());

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
