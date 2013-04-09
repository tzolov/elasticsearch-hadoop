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
package org.elasticsearch.hadoop.cascading;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * Hadoop-based Cascading Tap.
 */
@SuppressWarnings("rawtypes")
class ESHadoopTap extends Tap<JobConf, RecordReader, OutputCollector> {

    private String target;

    public ESHadoopTap(String host, int port, String index, Fields fields) {
        this.target = index;
        setScheme(new ESHadoopScheme(host, port, index, fields));
    }

    @Override
    public String getIdentifier() {
        return target;
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess, RecordReader input) throws IOException {
        return new HadoopTupleEntrySchemeIterator(flowProcess, this, input);
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess, OutputCollector output) throws IOException {
        return new HadoopTupleEntrySchemeCollector(flowProcess, this, output);
    }

    @Override
    public boolean createResource(JobConf conf) throws IOException {
        return false;
    }

    @Override
    public boolean deleteResource(JobConf conf) throws IOException {
        return false;
    }

    @Override
    public boolean resourceExists(JobConf conf) throws IOException {
        return true;
    }

    @Override
    public long getModifiedTime(JobConf conf) throws IOException {
        return 0;
    }
}