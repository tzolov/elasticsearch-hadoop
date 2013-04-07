# ElasticSearch Hadoop
Read and write data to/from ElasticSearch within Hadoop/MapReduce libraries. Automatically converts data to/from JSON. Supports [MapReduce](#mapreduce), [Cascading](#cascading), [Hive](#hive) and [Pig](#pig).

# Requirements
ElasticSearch cluster accessible through [REST][]. That's it!
Significant effort has been invested to create a small, dependency-free, self-contained jar that can be downloaded and put to use without any dependencies. Simply make it available to your job classpath and you're set.

# License
This project is released under version 2.0 of the [Apache License][]

# Installation
We're working towards a first release and we plan to have nightly builds soon available. In the meantime please [build](#building-the-source) the project yourself.

# Feedback / Q&A
We're interested in your feedback! You can find us on the User [mailing list](https://groups.google.com/forum/?fromgroups#!forum/elasticsearch) - please append `[Hadoop]` to the post subject to filter it out. For more details, see the [community](http://www.elasticsearch.org/community/) page.

# Usage

## Configuration Properties

These properties are read from Hadoop Configuration if the user hasn't specified them.
```
es.host=<ES host address> # optional, defaults to localhost
es.port=<ES REST port>    # optional, defaults to 9200
es.location=<ES resource location, relative to the host/port specified above. Can be an index or a query> # required
```

## [MapReduce][]
For basic, low-level or performance-sensitive environments, ES-Hadoop provides dedicated `InputFormat` and `OutputFormat` that read and write data to ElasticSearch. To use them, add the `es-hadoop` jar to your job classpath
(either by bundling the library along - it's less then 40kB and there are no-dependencies), using the [DistributedCache][] or by provisioning the cluster manually.

Note that es-hadoop supports both the so-called 'old' and the 'new' API through its `ESInputFormat` and `ESOutputFormat` classes.

### 'Old' (`org.apache.hadoop.mapred`) API

### Reading
To read data from ES, configure the `ESInputFormat` on your job configuration along with the relevant [properties](#configuration-properties):
```java
JobConf conf = new JobConf();
conf.setInputFormat(ESInputFormat.class);
conf.set("es.location", "radio/artists/_search?q=me*"); // replace this with the relevant query
...
JobClient.runJob(conf);
```
### Writing
Same configuration template can be used for writing but using `ESOuputFormat`:
```java
JobConf conf = new JobConf();
conf.setOutputFormat(ESOutputFormat.class);
conf.set("es.location", "radio/artists"); // index or indices used for storing data
...
JobClient.runJob(conf);
```
### 'New' (`org.apache.hadoop.mapreduce`) API

### Reading
```java
Configuration conf = new Configuration();
conf.set("es.location", "radio/artists/_search?q=me*"); // replace this with the relevant query
Job job = new Job(conf)
job.setInputFormat(ESInputFormat.class);
...
job.waitForCompletion(true);
```
### Writing
```java
Configuration conf = new Configuration();
conf.set("es.location", "radio/artists"); // index or indices used for storing data
Job job = new Job(conf)
job.setOutputFormat(ESOutputFormat.class);
...
job.waitForCompletion(true);
```
## [Hive][]
ES-Hadoop provides a Hive storage handler for ElasticSearch, meaning one can define an [external table][] on top of ES.

Add es-hadoop-<version>.jar to `hive.aux.jars.path` or register it manually in your Hive script (recommended):
```
ADD JAR /path_to_jar/es-hadoop-<version>.jar;
```
### Reading
To read data from ES, define a table backed by the desired index:
```SQL
CREATE EXTERNAL TABLE artists (
    id      BIGINT,
    name    STRING,
    links   STRUCT<url:STRING, picture:STRING>)
STORED BY 'org.elasticsearch.hadoop.hive.ESStorageHandler'
TBLPROPERTIES('es.location' = 'radio/artists/_search?q=me*');
```
The fields defined in the table are mapped to the JSON when communicating with ElasticSearch. Notice the use of `TBLPROPERTIES` to define the location, that is the query used for reading from this table:
```
SELECT * FROM artists;
```

### Writing
To write data, a similar definition is used but with a different `es.location`:
```SQL
CREATE EXTERNAL TABLE artists (
    id      BIGINT,
    name    STRING,
    links   STRUCT<url:STRING, picture:STRING>)
STORED BY 'org.elasticsearch.hadoop.hive.ESStorageHandler'
TBLPROPERTIES('es.location' = 'radio/artists/');
```

Any data passed to the table is then passed down to ElasticSearch; for example considering a table `s`, mapped to a TSV/CSV file, one can index it to ElasticSearch like this:
```SQL
INSERT OVERWRITE TABLE artists 
    SELECT NULL, s.name, named_struct('url', s.url, 'picture', s.picture) FROM source s;
```

As one can note, currently the reading and writing are treated separately but we're working on unifying the two and automatically translating [HiveQL][] to ElasticSearch queries.

## [Pig][]
ES-Hadoop provides both read and write functions for Pig so you can access ElasticSearch from Pig scripts.

Register ES-Hadoop jar into your script or add it to your Pig classpath:
```
REGISTER /path_to_jar/es-hadoop-<version>.jar;
```
Additionally one can define an alias to save some chars:
```
%define ESSTORAGE org.elasticsearch.hadoop.pig.ESStorage()
```
and use `$ESSTORAGE` for storage definition.

### Reading
To read data from ES, use `ESStorage` and specify the query through the `LOAD` function:
```
A = LOAD 'radio/artists/_search?q=me*' USING org.elasticsearch.hadoop.pig.ESStorage();
DUMP A;
```

### Writing
Use the same `Storage` to write data to ElasticSearch:
```
A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, url:chararray, picture: chararray);
B = FOREACH A GENERATE name, TOTUPLE(url, picture) AS links;
STORE B INTO 'radio/artists' USING org.elasticsearch.hadoop.pig.ESStorage();
```

## [Cascading][]
ES-Hadoop offers a dedicate ElasticSearch [Tap][], `ESTap` that can be used both as a sink or a source. Note that `ESTap` can be used in both local (`LocalFlowConnector`) and Hadoop (`HadoopFlowConnector`) flows:

### Reading
```java
Tap in = new ESTap("radio/artists/_search?q=me*");
Tap out = new StdOut(new TextLine());
new LocalFlowConnector().connect(in, out, new Pipe("read-from-ES")).complete();
```
### Writing
```java
Tap in = Lfs(new TextDelimited(new Fields("id", "name", "url", "picture")), "src/test/resources/artists.dat");
Tap out = new ESTap("radio/artists", new Fields("name", "url", "picture"));
new HadoopFlowConnector().connect(in, out, new Pipe("write-to-ES")).complete();
```

## [Crunch][]
ES-Hadoop provides ElasticSearch [Source][] (`ESSource`) and [Target][] (`ESTarget`) for reading and writing ElasticSearch indexes.

```
Note: Only the Crunch's WritableTypeFamily is supported! 
```

Annotated sample application is provided here: [ESCrunchIntegartionTest][].

### Reading
```java
ESSource esSource =  new ESSource.Builder("twitter/tweet/_search?q=user:*").setHost("localhost").setPort(9200).build();
MRPipeline pipeline = new MRPipeline(...);
PCollection<MapWritable> tweets = pipeline.read(esSource);
...
```
The result is a collection of `MapWritable` elements - one element per ES `source` object. ES-Hadoop uses `MapWritable` to represent the input JSON data.

### Writing
To writhe
```java
PCollection<MyJsonOutputSchema> myJsonOutputCollection = ...
ESTarget esTarget = new ESTarget.Builder("twitter/count/").setHost("localhost").setPort(9200).build();
pipeline.write(myJsonOutputCollection, esTarget);
```
The output JSON format is defined via a custom Java class. It relies on Jackson's object serialization (part of the ES-Hadoop `RestClient`) 
to convert the output data into JSON source objects stored in ES. 

```
Note: The custom Java class has to implement the `Writable` interface to fit with Crunch's WritableTypeFamily.
Despite of this you can leave empty the interface methods implementation. 
```
Sample Java class used to define the output JSON format.   
```java
public class MyJsonOutputSchema implements Writable, Serializable {

  private String userName;

  public String getUserName() { return userName; }

  public void setUserName(String userName) { this.userName = userName;}
  
  @Override
  public void readFields(DataInput arg0) throws IOException {
    // Not required for the ES output
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    // Not required for the ES output
  }
}
```

# Building the source

ElasticSearch Hadoop uses [Gradle][] for its build system and it is not required to have it installed on your machine.

To create a distributable jar, run `gradlew -x test build` from the command line; once completed you will find the jar in `build\libs`.

```
Note: Crunch integration requires latest trunk code (0.6.0-SNAPSHOT). Therefore clone, build and install Crunch in your local Maven repository:
 $ git clone http://git-wip-us.apache.org/repos/asf/crunch.git
 $ mvn clean install -DskipTests -Drat.numUnapprovedLicenses=1000
```

[Hadoop]: http://hadoop.apache.org
[MapReduce]: http://hadoop.apache.org/docs/r1.0.4/mapred_tutorial.html
[Pig]: http://pig.apache.org
[Hive]: http://hive.apache.org
[HiveQL]: http://cwiki.apache.org/confluence/display/Hive/LanguageManual
[external table]: http://cwiki.apache.org/Hive/external-tables.html
[Apache License]: http://www.apache.org/licenses/LICENSE-2.0
[Gradle]: http://www.gradle.org/
[REST]: http://www.elasticsearch.org/guide/reference/api/
[DistributedCache]: http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/filecache/DistributedCache.html
[Cascading]: http://www.cascading.org/
[Tap]: http://docs.cascading.org/cascading/2.1/userguide/html/ch03s05.html
[Crunch]: http://crunch.apache.org
[Source]: http://crunch.apache.org/apidocs/0.5.0/org/apache/crunch/Source.html
[Target]: http://crunch.apache.org/apidocs/0.5.0/org/apache/crunch/Target.html
[ESCrunchIntegartionTest]: https://github.com/tzolov/elasticsearch-hadoop/blob/master/src/test/java/org/elasticsearch/hadoop/crunch/ESCrunchIntegartionTest.java
