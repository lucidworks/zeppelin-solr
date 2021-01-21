package com.lucidworks.zeppelin.solr;

import com.lucidworks.zeppelin.solr.query.StreamingExpressionResultIterator;
import com.lucidworks.zeppelin.solr.query.StreamingResultsIterator;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Interpreter for Apache Solr Search engine
 */
public class SolrInterpreter extends Interpreter {

  private static Logger logger = LoggerFactory.getLogger(SolrInterpreter.class);
  public static final String BASE_URL = "solr.baseUrl";
  public static final String COLLECTION = "solr.collection";
  public static final String JDBC_URL = "jdbc.url";
  public static final String JDBC_DRIVER = "jdbc.driver";

  private String baseURL;
  private HttpSolrClient solrClient;
  public SolrInterpreter(Properties property) {
    super(property);
  }
  private String collection;
  private SolrLukeResponse lukeResponse;

  private static final List<String> COMMANDS = Arrays.asList(
      "help", "use", "search", "facet", "stream", "sql");
  private static final String COMMAND_LIST_STRING = "[" + String.join(", ", COMMANDS) + "]";

  @ZeppelinApi
  public void open() {
    baseURL = getProperty(BASE_URL);
    logger.info("Connecting to Solr host {}", baseURL);
    collection = getProperty(COLLECTION);
    if(collection != null) {
      //Set the base collection but don't do the Luke response.
      //The get the luke response uses must specify use.
      //Streaming Expressions and SQL don't require a luke response. They just require a collection and client
      //So "use" is not required for streaming expressions or sql if default collection is set.
      solrClient = SolrSupport.getNewHttpSolrClient(baseURL+"/"+collection);
    }
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

    if ("help".equals(args[0])) {
      return determineHelpResponse(args);
    } else if ("use".equals(args[0])) {
      if (args.length == 2) {
        collection = args[1];
        if(solrClient != null) {
          try {
            solrClient.close();
          } catch (Exception e) {
            logger.error("Error closing connection", e);
          }
        }
        solrClient = SolrSupport.getNewHttpSolrClient(baseURL+"/"+collection);
        lukeResponse = SolrQuerySupport.getFieldsFromLuke(solrClient, collection);
        InterpreterResult result = new InterpreterResult(InterpreterResult.Code.SUCCESS);
        result.add(InterpreterResult.Type.TEXT,  "Setting collection " + collection + " as default");
        return result;
      } else {
        String msg = "Specify the collection to use for this dashboard. Example: use {collection_name}";
        return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, msg);
      }
    }

    if(collection == null) {
      String msg = "Specify the collection to use for this dashboard. Example: use {collection_name}";
      return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, msg);
    }

    if ("search".equals(args[0])) {
      if(lukeResponse == null) {
        return returnCollectionNull();
      }

      if (args.length == 2) {
        SolrQuery searchSolrQuery = SolrQuerySupport.toQuery(args[1]);
        try {
          return SolrQuerySupport.doSearchQuery(searchSolrQuery, lukeResponse, solrClient, searchSolrQuery.get("collection", collection));
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
      if(lukeResponse == null) {
        return returnCollectionNull();
      }
      if (args.length == 2) {
        SolrQuery searchSolrQuery = SolrQuerySupport.toQuery(args[1]);
        if (collection == null) {
          return returnCollectionNull();
        }
        try {
          return SolrQuerySupport.doFacetQuery(searchSolrQuery, solrClient, searchSolrQuery.get("collection", collection));
        } catch (Exception e) {
          return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, e.getMessage());
        }
      } else {
        String msg = "Specify the query params to facet with. Example: facet q=text&facet.field=genre&collection=solr_collection";
        return new InterpreterResult(InterpreterResult.Code.INCOMPLETE, InterpreterResult.Type.TEXT, msg);
      }
    }

    if (isStreamOrSql(args[0])) {
      if (collection == null) returnCollectionNull();
      if (args.length > 1 || args[0].contains("(")) {
        if(args[0].contains("(")) {
          try {
            st = addJDBCParams(st);
            StreamingExpressionResultIterator streamingResultsIterator = SolrQuerySupport.doStreamingIterator("stream "+st, solrClient, collection, "stream");
            return getStreamingResult(streamingResultsIterator);
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
            StreamingExpressionResultIterator streamingResultsIterator = SolrQuerySupport.doStreamingIterator(st, solrClient, collection, args[0]);
            return getStreamingResult(streamingResultsIterator);
            //return SolrQuerySupport.doStreamingQuery(st, solrClient, collection, args[0]);
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

  private InterpreterResult getStreamingResult(StreamingExpressionResultIterator streamingExpressionResultIterator) {

    InterpreterResult interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS);

    List<Map> tuples = new ArrayList();
    Set<String> fields = new HashSet();
    while(streamingExpressionResultIterator.hasNext()) {
      Map tuple = streamingExpressionResultIterator.nextTuple();
      tuples.add(tuple);
      for(Object field : tuple.keySet()) {
        fields.add(field.toString());
      }
    }

    StringBuilder builder = new StringBuilder();
    for(String field : fields) {
      if(builder.length() > 0) {
        builder.append("\t");
      }
      builder.append(field);
    }
    builder.append("\n");

    for(Map tuple : tuples) {
      boolean tab = false;
      for(String field : fields) {
        if(tab) {
          builder.append("\t");
        } else {
          tab = true;
        }

        Object value = tuple.get(field);
        if(value != null) {
          builder.append(value.toString());
        }
      }
      builder.append("\n");
    }

    if (streamingExpressionResultIterator.getNumDocs() == 0) {
      interpreterResult.add(InterpreterResult.Type.HTML, "<font color=red>Zero results for the query.</font>");
      return interpreterResult;
    } else {
      interpreterResult.add(InterpreterResult.Type.TABLE, builder.toString());
      interpreterResult.add(InterpreterResult.Type.HTML, "<font color=blue>Number of results:"+streamingExpressionResultIterator.getNumDocs()+".</font>");
      return interpreterResult;
    }
  }

  public String addJDBCParams(String expr) {

    if(expr.contains("jdbc(") && properties.containsKey(JDBC_URL) && !expr.contains("connection=")) {
      String url = properties.getProperty(JDBC_URL);
      String driver = properties.getProperty(JDBC_DRIVER);
      String JDBCParams = "connection=\"" + url + "\", driver=\"" + driver + "\", ";
      if(!expr.contains("jdbc(sort=")) {
        JDBCParams = "sort=\"id desc\", " + JDBCParams;
      }
      expr = expr.replace("jdbc(", "jdbc(" + JDBCParams);
    }

    return expr;
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

  // Only called when args[0].equals("help")
  private InterpreterResult determineHelpResponse(String[] args) {
    String msg = null;
    if (args.length == 1) {
      msg = "List of supported commands: " + COMMAND_LIST_STRING + ". Run `help <command>` for information on a specific command";
    } else if (args.length == 2) {
      switch (args[1]){
        case "use":
          msg = "Set a default collection for use in other commands.\\nUsage: `use <collection_name>`";
          break;
        case "search":
          msg = "Issue a search request and display the results.\\nUsage: `search <Solr query params>`";
          break;
        case "facet":
          msg = "Issue a facet request and display the computed counts.\\n Usage: `facet <Solr facet params>`";
          break;
        case "stream":
          msg = "Issue a streaming expression request and display the results.\\nUsage: `stream <streaming-expression>`";
          break;
        case "sql":
          msg = "Issue a SQL query and display the results.  NOTE: Solr only supports a subset of traditional SQL " +
                  "syntax.  See the Solr Reference Guide for details. " +
                  "https://lucene.apache.org/solr/guide/parallel-sql-interface.html#solr-sql-syntax" +
                  "\\nUsage: `sql <sql-expression>`";
          break;
        default:
          msg = "Command [" + args[1] + "] not supported.  Supported commands are " + COMMAND_LIST_STRING + ".";
          return new InterpreterResult(InterpreterResult.Code.ERROR, InterpreterResult.Type.TEXT, msg);
      }
    }
    return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TEXT, msg);
  }
}
