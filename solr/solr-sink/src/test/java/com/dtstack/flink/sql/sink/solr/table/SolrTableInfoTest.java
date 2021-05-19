package com.dtstack.flink.sql.sink.solr.table;

import com.dtstack.flink.sql.sink.solr.options.SolrClientOptions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SolrTableInfoTest {

    @Test
    public void check() {
        SolrTableInfo solrTableInfo = new SolrTableInfo();
        List<String> zkHosts = new ArrayList<>();
        SolrClientOptions solrClientOptions = new SolrClientOptions(zkHosts, null, "");
        solrTableInfo.setSolrClientOptions(solrClientOptions);
        try {
            solrTableInfo.check();
        } catch (NullPointerException | IllegalStateException e) {
        }
        zkHosts.add("host:2181");
        solrClientOptions.setZkHosts(zkHosts);
        try {
            solrTableInfo.check();
        } catch (NullPointerException | IllegalStateException e) {
        }
        solrClientOptions.setCollection("abc");
        solrTableInfo.check();
    }
}
