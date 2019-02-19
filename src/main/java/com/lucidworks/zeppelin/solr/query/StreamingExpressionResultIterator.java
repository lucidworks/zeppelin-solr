package com.lucidworks.zeppelin.solr.query;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;


public class StreamingExpressionResultIterator extends TupleStreamIterator {

  private static final Logger log = Logger.getLogger(StreamingExpressionResultIterator.class);

  protected String collection;
  protected String qt;
  protected String baseURL;


  public StreamingExpressionResultIterator(String baseURL, String collection, SolrParams solrParams) {
    super(solrParams);
    this.baseURL = baseURL;
    this.collection = collection;

    qt = solrParams.get(CommonParams.QT);
    if (qt == null) qt = "/stream";
  }

  protected TupleStream openStream() {
    TupleStream stream;

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, qt);
    params.set("collection", collection);

    String aggregationMode = solrParams.get("aggregationMode");

    log.info("aggregationMode=" + aggregationMode + ", solrParams: " + solrParams);
    if (aggregationMode != null) {
      params.set("aggregationMode", aggregationMode);
    } else {
      params.set("aggregationMode", "facet"); // use facet by default as it is faster
    }
    
    if ("/sql".equals(qt)) {
      String sql = solrParams.get("sql").replaceAll("\\s+", " ");
      log.info("Executing SQL statement " + sql + " against collection " + collection);
      params.set("stmt", sql);
    } else {
      String expr = solrParams.get("expr").replaceAll("\\s+", " ");
      log.info("Executing streaming expression " + expr + " against collection " + collection);
      params.set("expr", expr);
    }

    try {
      log.info("Sending "+qt+" request to replica "+baseURL+" of "+collection+" with params: "+params);
      long startMs = System.currentTimeMillis();
      stream = new SolrStream(baseURL, params);
      stream.setStreamContext(getStreamContext());
      stream.open();
      long diffMs = (System.currentTimeMillis() - startMs);
      log.info("Open stream to "+baseURL+" took "+diffMs+" (ms)");
    } catch (Exception e) {
      log.error("Failed to execute request ["+solrParams+"] due to: "+e, e);
      if (e instanceof RuntimeException) {
        throw (RuntimeException)e;
      } else {
        throw new RuntimeException(e);
      }
    }
    return stream;
  }

  protected StreamContext getStreamContext() {
    StreamContext context = new StreamContext();
    return context;
  }

}
