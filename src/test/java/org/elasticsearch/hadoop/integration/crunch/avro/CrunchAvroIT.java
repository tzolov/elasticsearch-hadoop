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
package org.elasticsearch.hadoop.integration.crunch.avro;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.crunch.PCollection;
import org.apache.crunch.fn.IdentityFn;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.At;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.elasticsearch.hadoop.crunch.ESTarget;
import org.elasticsearch.hadoop.crunch.ESSource;
import org.elasticsearch.hadoop.integration.LocalES;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.Lists;

public class CrunchAvroIT implements Serializable {

  @ClassRule
  public static LocalES esServer = new LocalES();

  transient AvroTypeFamily tf;

  private File avroFile;

  @Before
  public void before() throws IOException {
    avroFile = File.createTempFile("test", ".avro");
    avroFile.deleteOnExit();

    // Populate a test Avro file with sample data
    populateGenericFile(
        avroFile,
        Lists.newArrayList(createPerosn("Christian Tzolov", 30, Lists.newArrayList("Chris", "Ian")),
            createPerosn("John Atanasoff", 91, Lists.newArrayList("Computer father")),
            createPerosn("Kiril-Victor Tsolov", 13, Lists.newArrayList("Kevin", "Kiko"))), Person.SCHEMA$);

    tf = AvroTypeFamily.getInstance();
  }

  @Test
  public void testWriteAvroToES() throws InterruptedException, IOException {

    // Create new Crunch pipeline
    MRPipeline pipeline = new MRPipeline(CrunchAvroIT.class);

    // 1. Read person from existing Avro file
    PCollection<Person> personCollection = pipeline.read(At.avroFile(avroFile.getAbsolutePath(),
        Avros.records(Person.class)));

    // 2. Write the person collection to ES index
    pipeline.write(personCollection, new ESTarget.Builder("person/avro").setPort(9700).build());

    assertTrue("Pipeline exectuion failed!", pipeline.done().succeeded());

    esServer.refresIndex("person");

    readPersonFromES();

    readAsTextFromES();

  }

  private void readAsTextFromES() {
    MRPipeline pipeline = new MRPipeline(CrunchAvroIT.class);

    IdentityFn<String> identityFn = IdentityFn.getInstance();

    PCollection<String> rawCollection = pipeline.read(
        new ESSource.Builder<String>("person/avro/_search?q=*", String.class).setPort(9700).build()).parallelDo(
        identityFn, tf.strings());

    ArrayList<String> list = Lists.newArrayList(rawCollection.materialize());

    System.out.println(list);
  }

  public void readPersonFromES() {

    IdentityFn<Person> identityFn = IdentityFn.getInstance();

    MRPipeline pipeline = new MRPipeline(CrunchAvroIT.class);

    PCollection<Person> people = pipeline.read(
        new ESSource.Builder<Person>("person/avro/_search?q=*", Person.class).setPort(9700).build()).parallelDo(
        identityFn, tf.records(Person.class));

    System.out.println(people.getPType().getFamily());
    System.out.println(Lists.newArrayList(pipeline.materialize(people)));
  }

  private GenericRecord createPerosn(String name, int age, ArrayList<String> siblingnames) {
    GenericRecord savedRecord = new GenericData.Record(Person.SCHEMA$);
    savedRecord.put("name", name);
    savedRecord.put("age", age);
    savedRecord.put("siblingnames", siblingnames);

    return savedRecord;
  }

  private void populateGenericFile(File avroFile, List<GenericRecord> genericRecords, Schema schema) throws IOException {

    FileOutputStream outputStream = new FileOutputStream(avroFile);
    GenericDatumWriter<GenericRecord> genericDatumWriter = new GenericDatumWriter<GenericRecord>(schema);

    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(genericDatumWriter);
    dataFileWriter.create(schema, outputStream);

    for (GenericRecord record : genericRecords) {
      dataFileWriter.append(record);
    }

    dataFileWriter.close();
    outputStream.close();

  }

}
