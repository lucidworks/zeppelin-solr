package com.lucidworks.zeppelin.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * Interpreter for Apache Solr Search engine
 */
public class SolrInterpreter extends Interpreter {

  private static Logger logger = LoggerFactory.getLogger(SolrInterpreter.class);
  public static final String ZK_HOST = "solr.zkhost";

  private String zkHost;
  private CloudSolrClient solrClient;
  private SolrLukeResponse lukeResponse;
  public SolrInterpreter(Properties property) {
    super(property);
  }
  private String collection;

  private static final List<String> COMMANDS = Arrays.asList(
      "list", "use", "search", "facet", "stream", "sql");

  @ZeppelinApi
  public void open() {
    zkHost = getProperty(ZK_HOST);
    logger.info("Connecting to Zookeeper host {}", zkHost);
    solrClient = SolrSupport.getCachedCloudClient(zkHost);
  }

  @ZeppelinApi
  public void close() {}

  @ZeppelinApi
  public InterpreterResult interpret(String st, InterpreterContext context) {
    logger.info("Running command '" + st + "'");

    if (st.isEmpty() || st.trim().isEmpty()) {
      return new InterpreterResult(InterpreterResult.Code.SUCCESS);
    }
    String[] args = st.split(" ");

    if ("list".equals(args[0])) {
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TEXT, SolrQuerySupport.getCollectionsListAsString(zkHost));
    }

    if ("use".equals(args[0])) {
      if (args.length == 2) {
        collection = args[1];
        lukeResponse = SolrQuerySupport.getFieldsFromLuke(zkHost, collection);
        InterpreterResult result = SolrQuerySupport.transformLukeResponseToInterpeterResponse(lukeResponse);
        if (result.code().equals(InterpreterResult.Code.SUCCESS)) {
          result.add(InterpreterResult.Type.TEXT,  "Setting collection " + collection + " as default");
        }
        return result;
      } else {
        String msg = "Specify the collection to use for this dashboard. Example: use {collection_name}";
        return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, msg);
      }
    }

    if ("search".equals(args[0])) {
      if (args.length == 2) {
        SolrQuery searchSolrQuery = SolrQuerySupport.toQuery(args[1]);
        String overrideCollection = searchSolrQuery.get("collection", null);
        if (overrideCollection == null && collection == null) return returnCollectionNull();
        try {
          if (overrideCollection != null) {
            return SolrQuerySupport.doSearchQuery(searchSolrQuery, SolrQuerySupport.getFieldsFromLuke(zkHost, overrideCollection), solrClient, overrideCollection);
          } else {
            return SolrQuerySupport.doSearchQuery(searchSolrQuery, lukeResponse, solrClient, collection);
          }
        } catch (Exception e) {
          logger.error("Exception processing query. Exception: " + e.getMessage());
          e.printStackTrace();
          return new InterpreterResult(InterpreterResult.Code.ERROR, InterpreterResult.Type.TEXT, "Error processing query. Exception: " + e.getMessage());
        }
      } else {
        String msg = "Specify the query params to search with. Example: search q=Fellas&fq=genre:action&collection=solr_collection";
        return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, msg);
      }
    }

    if ("facet".equals(args[0])) {
      if (args.length == 2) {
        SolrQuery searchSolrQuery = SolrQuerySupport.toQuery(args[1]);
        if (collection == null && searchSolrQuery.get("collection") == null) {
          return returnCollectionNull();
        }
        try {
          return SolrQuerySupport.doFacetQuery(searchSolrQuery, solrClient, searchSolrQuery.get("collection", collection));
        } catch (Exception e) {
          return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, e.getMessage());
        }
      } else {
        String msg = "Specify the query params to facet with. Example: search q=text&facet=true&facet.field=genre&collection=solr_collection";
        return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, msg);
      }
    }

    if (isStreamOrSql(args[0])) {
      if (collection == null) returnCollectionNull();
      if (args.length > 1 || args[0].contains("(")) {
        if(args[0].contains("(")) {
          try {
            return SolrQuerySupport.doStreamingQuery("stream "+st, solrClient, collection, "stream");
          } catch (Exception e) {
            return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, e.getMessage());
          }
        } else if(args[0].equalsIgnoreCase("select")) {
          try {
            return SolrQuerySupport.doStreamingQuery("sql "+st, solrClient, collection, "sql");
          } catch (Exception e) {
            return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, e.getMessage());
          }
        } else {
          try {
            return SolrQuerySupport.doStreamingQuery(st, solrClient, collection, args[0]);
          } catch (Exception e) {
            return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, e.getMessage());
          }
        }
      } else {
        String msg = "Specify the streaming expression. Example: stream {streaming expression}";
        return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, msg);
      }
    }

    return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, "Unknown command: " + st + ". List of allowed commands: " + COMMANDS);
  }

  public boolean isStreamOrSql(String arg) {
    return arg.equals("stream") || arg.equals("sql") || arg.contains("(") || arg.equalsIgnoreCase("select");
  }
  @Override
  public void cancel(InterpreterContext context) {}

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  public SolrClient getCloudClient() {
    return this.solrClient;
  }

  public InterpreterResult returnCollectionNull() {
      return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, "Set collection to use with 'use {collection}' command or set collection in query params for search and facet commands");
  }
}
