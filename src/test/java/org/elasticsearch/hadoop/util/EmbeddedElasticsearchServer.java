package org.elasticsearch.hadoop.util;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class EmbeddedElasticsearchServer {
  private static final String DEFAULT_DATA_DIRECTORY = "build/elasticsearch-data";

  private final Node node;
  private final String dataDirectory;

  public EmbeddedElasticsearchServer() {
    this(DEFAULT_DATA_DIRECTORY, false, false);
  }

  public EmbeddedElasticsearchServer(String dataDirectory, boolean isLocal, boolean isClientOnly) {
    this.dataDirectory = dataDirectory;

    ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder().put("path.data",
        dataDirectory);

    node = nodeBuilder().local(isLocal).client(isClientOnly).settings(elasticsearchSettings.build()).node();
  }

  public Client getClient() {
    return node.client();
  }

  public void shutdown() {
    node.close();
    deleteDataDirectory();
  }

  private void deleteDataDirectory() {
    try {
      FileUtils.deleteDirectory(new File(dataDirectory));
    } catch (IOException e) {
      throw new RuntimeException("Could not delete data directory of embedded elasticsearch server", e);
    }
  }

  public void refresIndex(String indexName) {
    new RefreshRequestBuilder(getClient().admin().indices()).setIndices(indexName).execute().actionGet();
  }

  public long countIndex(String indexName, String typeName) {
    return getClient().prepareCount(indexName).setTypes(typeName).execute().actionGet().count();
  }

  public Iterator<SearchHit> searchIndex(String indexName, String typeName) {
    return getClient().prepareSearch(indexName).setTypes(typeName).execute().actionGet().getHits().iterator();
  }
}