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
package org.elasticsearch.hadoop.pig;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.StoreMetadata;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;
import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.mr.ESInputFormat;
import org.elasticsearch.hadoop.mr.ESOutputFormat;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.WritableUtils;

/**
 * Pig storage for reading and writing data into an ElasticSearch index.
 * Uses the tuple implied schema to create the resulting JSON string sent to ElasticSearch.
 * <p/>
 * Typical usage is:
 *
 * <pre>
 * A = LOAD 'twitter/_search?q=kimchy' USING org.elasticsearch.hadoop.pig.ESStorage();
 * </pre>
 * <pre>
 * STORE A INTO '<index>' USING org.elasticsearch.hadoop.pig.ESStorage();
 * </pre>
 *
 * The ElasticSearch host/port can be specified through Hadoop properties (see package description)
 * or passed to the {@link #ESStorage(String, int)} constructor.
 */
public class ESStorage extends LoadFunc implements StoreFuncInterface, StoreMetadata {

    private static final Log log = LogFactory.getLog(ESStorage.class);
    private final boolean trace = log.isTraceEnabled();

    private final String host;
    private int port = 0;

    private String relativeLocation;
    private String signature;
    private ResourceSchema schema;
    private RecordReader<Text, MapWritable> reader;
    private RecordWriter<Object, Object> writer;

    public ESStorage() {
        this(null, "0");
    }

    public ESStorage(String host, String port) {
        this.host = host;
        this.port = Integer.valueOf(port);
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        SettingsManager.loadFrom(job.getConfiguration()).setHost(host).setPort(port).setResource(location).save();
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
        // TODO: do processing here
        return location;
    }

    @Override
    public OutputFormat<Object, Object> getOutputFormat() throws IOException {
        return new ESOutputFormat();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        this.writer = writer;

        Properties props = UDFContext.getUDFContext().getUDFProperties(getClass(), new String[] { signature });
        String s = props.getProperty(ResourceSchema.class.getName());
        this.schema = IOUtils.deserializeFromBase64(s);
    }

    // TODO: make put more lenient (if the schema is not available just shove everything on the existing type or as a big charray)
    @Override
    public void putNext(Tuple t) throws IOException {
        ResourceFieldSchema[] fields = schema.getFields();

        Map<String, Object> data = new LinkedHashMap<String, Object>(fields.length);

        for (int i = 0; i < fields.length; i++) {
            data.put(fields[i].getName(), PigTypeUtils.pigToObject(t.get(i), fields[i]));
        }

        if (trace) {
            log.trace("Writing out tuple " + data);
        }
        try {
            writer.write(null, data);
        } catch (InterruptedException ex) {
            throw new IOException("interrupted", ex);
        }
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        this.signature = signature;
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
        // no special clean-up required
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        // save schema to back-end for JSON translation
        Properties props = UDFContext.getUDFContext().getUDFProperties(getClass(), new String[] { signature });
        // save the schema as String (used JDK serialization since toString() screws up the signature - see the testcase)
        props.setProperty(ResourceSchema.class.getName(), IOUtils.serializeToBase64(s));
    }

    @Override
    public void storeStatistics(ResourceStatistics stats, String location, Job job) throws IOException {
        // no-op
    }

    @Override
    public void storeSchema(ResourceSchema schema, String location, Job job) throws IOException {
        // no-op
    }

    //
    // LoadFunc
    //

    public void setLocation(String location, Job job) throws IOException {
        SettingsManager.loadFrom(job.getConfiguration()).setHost(host).setPort(port).setResource(location).save();
    }


    @Override
    public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
        // TODO: potentially do additional parsing here
        relativeLocation = location;
        return relativeLocation;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public InputFormat getInputFormat() throws IOException {
        return new ESInputFormat();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = reader;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Tuple getNext() throws IOException {
        try {
            if (!reader.nextKeyValue()) {
                return null;
            }

            Map<Object, Object> data = (Map<Object, Object>) WritableUtils.fromWritable(reader.getCurrentValue());
            Tuple tuple = TupleFactory.getInstance().newTuple(data.size());
            int i = 0;
            for (Entry<Object, Object> entry : data.entrySet()) {
                tuple.set(i++, PigTypeUtils.objectToPig(entry.getValue()));
            }

            if (trace) {
                log.trace("Reading out tuple " + tuple);
            }
            return tuple;

        } catch (InterruptedException ex) {
            throw new IOException("interrupted", ex);
        }
    }

    // added in Pig 11.x
    public void cleanupOnSuccess(String location, Job job) throws IOException {
        //no-op
    }
}