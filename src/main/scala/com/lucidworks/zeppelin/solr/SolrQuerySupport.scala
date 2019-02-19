package com.lucidworks.zeppelin.solr

import java.net.URLDecoder

import com.lucidworks.zeppelin.solr.query.{StreamingExpressionResultIterator, StreamingResultsIterator}
import com.lucidworks.zeppelin.solr.util.QueryConstants
import org.apache.commons.lang.StringUtils
import org.apache.solr.client.solrj.SolrRequest.METHOD
import org.apache.solr.client.solrj._
import org.apache.solr.client.solrj.impl.{CloudSolrClient, HttpSolrClient, StreamingBinaryResponseParser}
import org.apache.solr.client.solrj.request.{CollectionAdminRequest, LukeRequest, QueryRequest}
import org.apache.solr.client.solrj.response.FacetField.Count
import org.apache.solr.client.solrj.response.LukeResponse.FieldInfo
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.common.params.SolrParams
import org.apache.solr.common.util.NamedList
import org.apache.zeppelin.interpreter.InterpreterResult
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.JavaConversions.{asScalaBuffer, mapAsScalaMap}
import scala.collection.mutable

case class SolrField(name: String, fieldType: String, docs: Int)
case class SolrLukeResponse(numDocs: Integer, fieldInfo: Map[String, (String, Long)])

case class SolrFieldMeta(
                          fieldType: String,
                          dynamicBase: Option[String],
                          isRequired: Option[Boolean],
                          isMultiValued: Option[Boolean],
                          isDocValues: Option[Boolean],
                          isStored: Option[Boolean],
                          fieldTypeClass: Option[String])

object SolrQuerySupport {
  val logger = LoggerFactory.getLogger(SolrQuerySupport.getClass)

  def getCollectionsList(zkHost: String): List[String] = {
    val solrClient = SolrSupport.getCachedCloudClient(zkHost)
    CollectionAdminRequest.listCollections(solrClient).asScala.toList
  }

  def getCollectionsListAsString(zkHost: String): String = {
    getCollectionsList(zkHost).mkString("\n")
  }

  def toQuery(queryString: String): SolrQuery = {

    var solrQuery: SolrQuery = new SolrQuery
    if (queryString == null || queryString.isEmpty) {
      solrQuery = solrQuery.setQuery("*:*")
    } else {
      // Check to see if the query contains additional parameters. E.g., q=*:*&fl=id&sort=id asc
      if (!queryString.contains("q=")) {
        // q= is required if passing list of name/value pairs, so if not there, whole string is the query
        solrQuery.setQuery(queryString)
      } else {
        val paramsNL = new NamedList[Object]()
        val params = queryString.split("&")
        for (param <- params) {
          // only care about the first equals as value may also contain equals
          val eqAt = param.indexOf('=')
          if (eqAt != -1) {
            val key = param.substring(0, eqAt)
            val value = URLDecoder.decode(param.substring(eqAt + 1), "UTF-8")
            if (key == "sort") {
              if (!value.contains(" ")) {
                solrQuery.addSort(SolrQuery.SortClause.asc(value))
              } else {
                val split = value.split(" ")
                solrQuery.addSort(SolrQuery.SortClause.create(split(0), split(1)))
              }
            } else {
              paramsNL.add(key, value)
            }
          }
        }
        if (paramsNL.size() > 0) {
          solrQuery.add(SolrParams.toSolrParams(paramsNL))
        }
      }
    }
    val rows = solrQuery.getRows
    if (rows == null)
      solrQuery.setRows(10)

    logger.info(s"Constructed SolrQuery: $solrQuery from user-supplied query param: $queryString")
    solrQuery
  }

  def querySolr(
      solrClient: SolrClient,
      solrQuery: SolrQuery,
      startIndex: Int,
      cursorMark: String): Option[QueryResponse] =
    querySolr(solrClient, solrQuery, startIndex, cursorMark, null)

  // Use this method instead of [[SolrClient.queryAndStreamResponse]] to use POST method for queries
  def queryAndStreamResponsePost(params: SolrParams, callback: StreamingResponseCallback, cloudClient: SolrClient): QueryResponse = {
    val parser: ResponseParser = new StreamingBinaryResponseParser(callback)
    val req: QueryRequest = new QueryRequest(params, METHOD.POST)
    req.setStreamingResponseCallback(callback)
    req.setResponseParser(parser)
    req.process(cloudClient)
  }

  /*
    Query solr and retry on Socket or network exceptions
   */
  def querySolr(
      solrClient: SolrClient,
      solrQuery: SolrQuery,
      startIndex: Int,
      cursorMark: String,
      callback: StreamingResponseCallback): Option[QueryResponse] = {
    var resp: Option[QueryResponse] = None

    var col = solrQuery.get("collection")



    try {
      if (cursorMark != null) {
        solrQuery.setStart(0)
        solrQuery.set("cursorMark", cursorMark)
        if (solrQuery.get("sort") == null || solrQuery.get("sort").isEmpty) {
          addDefaultSort(solrQuery, QueryConstants.DEFAULT_REQUIRED_FIELD)
        }
      } else {
        solrQuery.setStart(startIndex)
      }

      if (solrQuery.getRows == null)
        solrQuery.setRows(QueryConstants.DEFAULT_PAGE_SIZE)

      if (callback != null) {
        resp = Some(queryAndStreamResponsePost(solrQuery, callback, solrClient))
      } else {
        resp = Some(solrClient.query(solrQuery, METHOD.POST))
      }
    } catch {
      case e: Exception =>
        logger.error("Query [" + solrQuery + "] failed due to: " + e)

        //re-try once in the event of a communications error with the server
        if (SolrSupport.shouldRetry(e)) {
          try {
            Thread.sleep(2000L)
          } catch {
            case ie: InterruptedException => Thread.interrupted()
          }

          try {
            if (callback != null) {
              resp = Some(queryAndStreamResponsePost(solrQuery, callback, solrClient))
            } else {
              resp = Some(solrClient.query(solrQuery, METHOD.POST))
            }
          } catch {
            case execOnRetry: SolrServerException =>
              logger.error("Query on retry [" + solrQuery + "] failed due to: " + execOnRetry)
              throw execOnRetry
            case execOnRetry1: Exception =>
              logger.error("Query on retry [" + solrQuery + "] failed due to: " + execOnRetry1)
              throw new SolrServerException(execOnRetry1)
          }
        } else {
          e match {
            case e1: SolrServerException => throw e1
            case e2: Exception => throw new SolrServerException(e2)
          }
        }
    }
    resp
  }

  def addDefaultSort(solrQuery: SolrQuery, uniqueKey: String): Unit = {
    if (solrQuery.getSortField == null || solrQuery.getSortField.isEmpty) {
      solrQuery.addSort(SolrQuery.SortClause.asc(uniqueKey))
      logger.info(s"Added default sort clause on uniqueKey field $uniqueKey to query $solrQuery")
    }
  }

  def getNumDocsFromSolr(collection: String, zkHost: String, query: Option[SolrQuery]): Long = {
    val solrQuery = if (query.isDefined) query.get else new SolrQuery().setQuery("*:*")
    val cloneQuery = solrQuery.getCopy
    cloneQuery.set("distrib", "true")
    cloneQuery.setRows(0)
    val cloudClient = SolrSupport.getCachedCloudClient(zkHost)
    val response = cloudClient.query(collection, cloneQuery)
    response.getResults.getNumFound
  }

  def getFieldsFromLuke(solrClient : HttpSolrClient, collection: String): SolrLukeResponse = {
    val fieldMap: mutable.Map[String, (String, Long)] = mutable.Map.empty[String, (String, Long)]

    val fieldsFromLukeShard = getFieldsFromLukePerShard(solrClient, collection)
    fieldsFromLukeShard._1.foreach {
      case(fieldName, fieldInfo) =>
          fieldMap.put(fieldName, (fieldInfo.getType, fieldInfo.getDocs))
    }

    SolrLukeResponse(0, fieldMap.toMap)
  }

  def getFieldsFromLukePerShard(httpSolrClient: HttpSolrClient, collection : String): (Map[String, FieldInfo], Int) = {
    val lukeRequest = new LukeRequest()
    lukeRequest.setNumTerms(0)
    val lukeResponse = lukeRequest.process(httpSolrClient)
    if (lukeResponse.getStatus != 0) {
      throw new RuntimeException(
        "Solr request returned with status code '" + lukeResponse.getStatus + "'. Response: '" + lukeResponse.getResponse.toString)
    }
    (lukeResponse.getFieldInfo.toMap, lukeResponse.getNumDocs)
  }

  def transformLukeResponseToInterpeterResponse(lukeResponse: SolrLukeResponse): InterpreterResult = {
    val fieldInfo = lukeResponse.fieldInfo
    if (fieldInfo.isEmpty) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, InterpreterResult.Type.TEXT, "Empty luke response. Make sure you have documents in the collection")
    }
    val interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS)
    val stringBuilder = new StringBuilder
    stringBuilder.++=("Name\tType\tDocs\n")
    lukeResponse.fieldInfo.foreach {
      case(fieldName, (fieldType, docCount)) =>
        stringBuilder.++=(s"$fieldName\t$fieldType\t$docCount\t")
        stringBuilder.++=("\n")
    }
    interpreterResult.add(InterpreterResult.Type.TABLE, stringBuilder.toString())
    interpreterResult.add(InterpreterResult.Type.HTML, s"<font color=blue>Number of docs in collection: ${lukeResponse.numDocs}.</font>")
    interpreterResult
  }

  def doSearchQuery(
      solrQuery: SolrQuery,
      lukeResponse: SolrLukeResponse,
      solrClient: SolrClient,
      collection: String): InterpreterResult = {
    solrQuery.set("collection", collection)
    val streamingResultsIterator = new StreamingResultsIterator(solrClient, solrQuery)
    val interpreterResult: InterpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS)
    val stringBuilder = new StringBuilder

    if (solrQuery.getRows != null) {
      streamingResultsIterator.setMaxSampleDocs(solrQuery.getRows)
    } else {
      streamingResultsIterator.setMaxSampleDocs(10)
    }

    val userFields = solrQuery.getFields
    var fieldsList = lukeResponse.fieldInfo.keySet.toList
    if (userFields != null) {
      val userFieldsArray = List(userFields.split(","):_*)
      logger.info(s"User requested fields ${userFieldsArray}")
      fieldsList = fieldsList.filter(p => userFieldsArray.contains(p))
      if (userFieldsArray.contains("score")) {
        fieldsList =  "score" :: fieldsList
      }
    }

    fieldsList.zipWithIndex.foreach( sf => {
      if (sf._2 != fieldsList.size-1) {
        stringBuilder.++=(s"${sf._1}\t")
      } else {
        stringBuilder.++=(s"${sf._1}\n")
      }
    })
    while (streamingResultsIterator.hasNext) {
      val doc = streamingResultsIterator.next()
      fieldsList.zipWithIndex.foreach( sf => {
        val fieldValues: java.util.Collection[_] = doc.getFieldValues(sf._1)
        if (sf._2 != fieldsList.size-1) {
          stringBuilder.++=(StringUtils.join(fieldValues, ","))
          stringBuilder.++=("\t")
        } else {
          stringBuilder.++=(StringUtils.join(fieldValues, ","))
          stringBuilder.++=("\n")
        }
      })
    }
    if (streamingResultsIterator.getNumDocs > 0) {
      interpreterResult.add(InterpreterResult.Type.TABLE, stringBuilder.toString())
      interpreterResult.add(InterpreterResult.Type.HTML, s"<font color=blue>Number of results: ${streamingResultsIterator.getNumDocs}.</font>")
    } else {
      logger.info(s"Zero results for query ${solrQuery}")
      interpreterResult.add(InterpreterResult.Type.HTML, s"<font color=red>Zero results for the query.</font>")
    }
    interpreterResult
  }

  def doFacetQuery(
      solrQuery: SolrQuery,
      solrClient: SolrClient,
      collection: String): InterpreterResult = {
    solrQuery.setRows(0)
    solrQuery.setFacet(true)

    val queryResponse = solrClient.query(solrQuery)

    if (queryResponse.getStatus != 0) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, InterpreterResult.Type.TEXT, s"Non zero status. Response: ${queryResponse.getResponse.toString}")
    }

    val facetFields = queryResponse.getFacetFields
    if (facetFields == null || facetFields.isEmpty) {
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.HTML, s"<font color=red>No facets to display.</font>")
    }

    // Take the data from first facet field. It doesn't make sense to display multiple facets (for now, atleast)
    val facetField = facetFields.get(0)
    val fieldName = facetField.getName
    if (facetField.getValueCount == 0) {
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.HTML, s"<font color=red>No facet values to display for field ${fieldName}")
    }
    val stringBuilder = new StringBuilder
    stringBuilder.++=(s"${fieldName}\tCount\n")
    for (value: Count <- asScalaBuffer(facetField.getValues)) {
      stringBuilder.++=(s"${value.getName}\t${value.getCount}\n")
    }

    val result = new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE, stringBuilder.toString())
    result.add(InterpreterResult.Type.HTML, s"Displaying ${facetField.getValueCount} values")
    result
  }

  def doStreamingQuery(
      streamingExpression: String,
      solrClient: HttpSolrClient,
      collection: String,
      queryType: String): InterpreterResult = {
    val query = new SolrQuery()
    if (queryType.equals("stream")) {
      query.set("expr", streamingExpression.substring(queryType.length+1).replaceAll("\\s+", " "))
      query.set("qt", "/stream")
      logger.info(s"Solr query with streaming expression: ${query}")
    } else if (queryType.equals("sql")) {
      query.set("sql", streamingExpression.substring(queryType.length+1).replaceAll("\\s+", " "))
      query.set("qt", "/sql")
      logger.info(s"Solr query with SQL statement: ${query}")
    }

    val streamingIterator = new StreamingExpressionResultIterator(solrClient.getBaseURL, collection, query)

    val stringBuilder = new StringBuilder


    while (streamingIterator.hasNext) {
      val doc = mapAsScalaMap(streamingIterator.next())
      // Get field names from first doc and add them as headers
      if (streamingIterator.getNumDocs == 1) {
        doc.keySet.foreach(s => stringBuilder.++=(s"${s}\t"))
        if (stringBuilder.length > 0) {
          stringBuilder.deleteCharAt(stringBuilder.length - 1); // This efficiently removes the trailing tab.
        }
        stringBuilder.++=(s"\n")
      }

      doc.foreach(f => {
        f._2 match {
          case ul: java.util.Collection[_] => stringBuilder.++=(s"${StringUtils.join(ul, ",")}\t")
          case m: java.util.Map[_, _] => logger.info("Map ignored") // ignore maps for now
          case a: AnyRef => stringBuilder.++=(s"${String.valueOf(a)}\t")
          case _ => stringBuilder.++=("")
        }
      })
      if (stringBuilder.length > 0 && stringBuilder.charAt(stringBuilder.length - 1) == '\t') {
        stringBuilder.deleteCharAt(stringBuilder.length - 1); // This efficiently removes the trailing tab.
      }
      stringBuilder.++=(s"\n")
    }
    if (logger.isDebugEnabled()) {
      logger.debug(s"output: ${stringBuilder.toString()}")
    }
    val interpreterResult: InterpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS)
    if (streamingIterator.getNumDocs == 0) {
      interpreterResult.add(InterpreterResult.Type.HTML, s"<font color=red>Zero results for the query.</font>")
    } else {
      interpreterResult.add(InterpreterResult.Type.TABLE, stringBuilder.toString())
      interpreterResult.add(InterpreterResult.Type.HTML, s"<font color=blue>Number of results: ${streamingIterator.getNumDocs}.</font>")
    }
    interpreterResult
  }
}
