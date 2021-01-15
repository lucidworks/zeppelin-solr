package com.lucidworks.zeppelin.solr

import java.util.Properties

import org.apache.zeppelin.interpreter.InterpreterResult

class SolrInterpreterCommandsTest extends CollectionSuiteBuilder {



  test("Test use command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    val result = solrInterpreter.interpret(s"use ${collections(0)}", null)
    assert(result.code().eq(InterpreterResult.Code.SUCCESS))
  }



  test("Test search command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    solrInterpreter.interpret(s"use ${collections(0)}", null)
    val result = solrInterpreter.interpret(s"search q=*:*", null)
    assert(result.code().eq(InterpreterResult.Code.SUCCESS))
    assert(result.message().size() == 2)

    val msgs = result.message()
    val table = msgs.get(0)
    assert(table.getType.eq(InterpreterResult.Type.TABLE))
    val tableData = table.getData.split("\n")
    assert(tableData.size == 11) // 10 docs + header_
    val header = tableData(0)
    val headerFields = header.split("\t")
    assert(headerFields.size == 7)

    tableData.foreach(td => {
      val contents = td.split("\t")
      assert(contents.size == 7)
      assert(contents.forall(f => {
        f != null && f.length > 0
      } ))
    })
  }

  test("Zero-result queries should be repeatable") {
    val properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    solrInterpreter.interpret(s"use ${collections(0)}", null)
    val firstNoResultsResponse = solrInterpreter.interpret(s"search q=field1_s:nonexistent_value", null)
    assert(firstNoResultsResponse.code().eq(InterpreterResult.Code.SUCCESS))
    assert(firstNoResultsResponse.message().size() == 1)
    assert(firstNoResultsResponse.message().get(0).getData.equals("<font color=red>Zero results for the query.</font>"))

    val secondNoResultsResponse = solrInterpreter.interpret(s"search q=field1_s:nonexistent_value", null)
    assert(secondNoResultsResponse.code().eq(InterpreterResult.Code.SUCCESS))
    assert(secondNoResultsResponse.message().size() == 1)
    assert(secondNoResultsResponse.message().get(0).getData.equals("<font color=red>Zero results for the query.</font>"))

  }


  test("Search commands on empty collections should be repeatable") {
    val properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    solrInterpreter.interpret(s"use ${emptyCollection}", null)
    val firstEmptyCollectionResponse = solrInterpreter.interpret(s"search q=*:*", null)
    assert(firstEmptyCollectionResponse.code().eq(InterpreterResult.Code.SUCCESS))
    assert(firstEmptyCollectionResponse.message().size() == 1)
    assert(firstEmptyCollectionResponse.message().get(0).getData.equals("<font color=red>Zero results for the query.</font>"))


    val secondEmptyCollectionResponse = solrInterpreter.interpret(s"search q=*:*", null)
    assert(secondEmptyCollectionResponse.code().eq(InterpreterResult.Code.SUCCESS))
    assert(secondEmptyCollectionResponse.message().size() == 1)
    assert(secondEmptyCollectionResponse.message().get(0).getData.equals("<font color=red>Zero results for the query.</font>"))

  }
  
  // Make sure the collection parameter is passed through to Solr
  test("Test search command specifing non existent collection fails") {
    val properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    solrInterpreter.interpret(s"use ${collections(0)}", null)
    val result = solrInterpreter.interpret(s"search q=*:*&collection=fake_collection", null)
    assert(result.code().eq(InterpreterResult.Code.ERROR))
    assert(result.message().size() == 1)

  }  

  test("Test search command preserves fl order") {
    val properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    solrInterpreter.interpret(s"use ${collections(0)}", null)
    val result = solrInterpreter.interpret(s"search q=*:*&fl=id,field3_i,field2_s,field5_ii", null)
    assert(result.code().eq(InterpreterResult.Code.SUCCESS))
    assert(result.message().size() == 2)

    val msgs = result.message()
    val table = msgs.get(0)
    assert(table.getType.eq(InterpreterResult.Type.TABLE))
    val tableData = table.getData.split("\n")
    assert(tableData.size == 11) // 10 docs + header_
    val header = tableData(0)
    val headerFields = header.split("\t")
    assert(headerFields.size == 4)
    assert(header.equals("id\tfield3_i\tfield2_s\tfield5_ii"))

  }




  test("Test facet command with use command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    solrInterpreter.interpret(s"use ${collections(0)}", null)
    val result = solrInterpreter.interpret(s"facet q=*:*&facet.field=field1_s", null)
    assert(result.code().eq(InterpreterResult.Code.SUCCESS))
    assert(result.message().size() == 2)
    val msgs = result.message()
    val table = msgs.get(0)
    assert(table.getType.eq(InterpreterResult.Type.TABLE))
    val tableData = table.getData.split("\n")
    assert(tableData.size == 21) // 10 docs + header_
    val header = tableData(0)
    val headerFields = header.split("\t")
    assert(headerFields.size == 2)
    assert(header.equals("field1_s\tCount"))
  }


  //Facet command must always have collection sert with use
  test("Test facet command without use command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    val result = solrInterpreter.interpret(s"facet q=*:*&facet.field=field1_s&collection=${collections(0)}", null)
    assert(result.code().eq(InterpreterResult.Code.INCOMPLETE))
  }



  test("Test stream command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    solrInterpreter.interpret(s"use ${collections(0)}", null)
    val result = solrInterpreter.interpret(s"""stream search(${collections(0)}, q="*:*", fl="field1_s,field3_i", sort="field1_s asc", qt="/export")""", null)
    assert(result.code().eq(InterpreterResult.Code.SUCCESS))
    assert(result.message().size() == 2)
    val msgs = result.message()
    val table = msgs.get(0)
    assert(table.getType.eq(InterpreterResult.Type.TABLE))
    val tableData = table.getData.split("\n")
    assert(tableData.size == 21) // 10 docs + header_
    val header = tableData(0)
    val headerFields = header.split("\t")
    assert(headerFields.size == 2)
    assert(header.equals("field1_s\tfield3_i"))
    tableData.foreach(td => {
      val contents = td.split("\t")
      assert(contents.size == 2)
      assert(contents.forall(f => {
        f != null && f.length > 0
      } ))
    })
  }

  test("Test stream command 3") {
    val properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    properties.put(SolrInterpreter.COLLECTION, collections(0))
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    val result = solrInterpreter.interpret(s"""search(${collections(0)}, q="*:*", fl="field1_s,field3_i", sort="field1_s asc", qt="/export")""", null)
    assert(result.code().eq(InterpreterResult.Code.SUCCESS))
    assert(result.message().size() == 2)
    val msgs = result.message()
    val table = msgs.get(0)
    assert(table.getType.eq(InterpreterResult.Type.TABLE))
    val tableData = table.getData.split("\n")
    assert(tableData.size == 21) // 10 docs + header_
    val header = tableData(0)
    val headerFields = header.split("\t")
    assert(headerFields.size == 2)
    assert(header.equals("field1_s\tfield3_i"))
    tableData.foreach(td => {
      val contents = td.split("\t")
      assert(contents.size == 2)
      assert(contents.forall(f => {
        f != null && f.length > 0
      } ))
    })
  }

  test("Test JDBC Param Injection") {
    var properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    properties.put(SolrInterpreter.JDBC_URL, "jdbc:hive//blah")
    properties.put(SolrInterpreter.JDBC_DRIVER, "HiveDrive")
    var solrInterpreter = new SolrInterpreter(properties)
    //Assert properties are added
    var expr = solrInterpreter.addJDBCParams("jdbc(sql=\"select a from b\")")
    assert(expr.equals("jdbc(sort=\"id desc\", connection=\"jdbc:hive//blah\", driver=\"HiveDrive\", sql=\"select a from b\")"))

    //Assert properties are not added
    expr = solrInterpreter.addJDBCParams("jdbc(connection=\"blah\", sql=\"select a from b\")")
    assert(expr.equals("jdbc(connection=\"blah\", sql=\"select a from b\")"))


    //Assert the sort is not touched
    expr = solrInterpreter.addJDBCParams("jdbc(sort=\"blah asc\", sql=\"select a from b\")")
    assert(expr.equals("jdbc(connection=\"jdbc:hive//blah\", driver=\"HiveDrive\", sort=\"blah asc\", sql=\"select a from b\")"))


    properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    solrInterpreter = new SolrInterpreter(properties)
    //Assert properties are not added as there are no properties.
    expr = solrInterpreter.addJDBCParams("jdbc(sql=\"select a from b\")")
    assert(expr.equals("jdbc(sql=\"select a from b\")"))
  }


  test("Test SQL command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    solrInterpreter.interpret(s"use ${collections(0)}", null)
    val result = solrInterpreter.interpret(s"""sql SELECT field4_ss, count(*) FROM ${collections(1)} GROUP BY field4_ss ORDER BY count(*) LIMIT 10""", null)
    assert(result.code().eq(InterpreterResult.Code.SUCCESS))
    assert(result.message().size() == 2)
    val msgs = result.message()
    val table = msgs.get(0)
    assert(table.getType.eq(InterpreterResult.Type.TABLE))
    val tableData = table.getData.split("\n")
    assert(tableData.size == 3) // 10 docs + header_
    val header = tableData(0)
    val headerFields = header.split("\t")
    assert(headerFields.size == 2)
    tableData.foreach(td => {
      val contents = td.split("\t")
      assert(contents.size == 2)
      assert(contents.forall(f => {
        f != null && f.length > 0
      } ))
    })
  }

  test("Test SQL command 2") {
    val properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    solrInterpreter.interpret(s"use ${collections(0)}", null)
    val result = solrInterpreter.interpret(s"""SELECT field4_ss, count(*) FROM ${collections(1)} GROUP BY field4_ss ORDER BY count(*) LIMIT 10""", null)
    assert(result.code().eq(InterpreterResult.Code.SUCCESS))
    assert(result.message().size() == 2)
    val msgs = result.message()
    val table = msgs.get(0)
    assert(table.getType.eq(InterpreterResult.Type.TABLE))
    val tableData = table.getData.split("\n")
    assert(tableData.size == 3) // 10 docs + header_
    val header = tableData(0)
    val headerFields = header.split("\t")
    assert(headerFields.size == 2)
    tableData.foreach(td => {
      val contents = td.split("\t")
      assert(contents.size == 2)
      assert(contents.forall(f => {
        f != null && f.length > 0
      } ))
    })
  }

}
