package com.lucidworks.zeppelin.solr

import java.util.Properties

import org.apache.zeppelin.interpreter.InterpreterResult

class SolrInterpreterCommandsTest extends CollectionSuiteBuilder {

  test("Test list command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.ZK_HOST, zkHost)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    val result = solrInterpreter.interpret("list", null)
    assert(result.code().eq(InterpreterResult.Code.SUCCESS))
    assert(result.message().size() == 1)
    assert(result.message().get(0) != null)
    assert(result.message().get(0).getType.eq(InterpreterResult.Type.TEXT))

    val cols = result.message().get(0).getData.split("\n").toSet
    assert(cols == collections.toSet)
  }

  test("Test use command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.ZK_HOST, zkHost)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    val result = solrInterpreter.interpret(s"use ${collections(0)}", null)
    assert(result.code().eq(InterpreterResult.Code.SUCCESS))
    assert(result.message().size() == 3)

    val msgs = result.message()
    val table = msgs.get(0)
    assert(table.getType.eq(InterpreterResult.Type.TABLE))
    val tableData = table.getData.split("\n")
    assert(tableData.size == 8) // 6 fields + _version_ + header
    assert(tableData(0).equals("Name\tType\tDocs"))
    tableData.foreach(td => {
      val contents = td.split("\t")
      assert(contents.size == 3)
      assert(contents.forall(f => f != null && f.length > 1))
    })
  }

  test("Test search command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.ZK_HOST, zkHost)
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

  test("Test search command without collection") {
    val properties = new Properties()
    properties.put(SolrInterpreter.ZK_HOST, zkHost)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    val result = solrInterpreter.interpret(s"search q=*:*", null)
    assert(result.code().eq(InterpreterResult.Code.INCOMPLETE))
  }

  test("Test facet command with use command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.ZK_HOST, zkHost)
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

  test("Test facet command without use command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.ZK_HOST, zkHost)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    val result = solrInterpreter.interpret(s"facet q=*:*&facet.field=field1_s&collection=${collections(0)}", null)
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

  test("Test stream command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.ZK_HOST, zkHost)
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

  test("Test stream command 2") {
    val properties = new Properties()
    properties.put(SolrInterpreter.ZK_HOST, zkHost)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    solrInterpreter.interpret(s"use ${collections(0)}", null)
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


  test("Test SQL command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.ZK_HOST, zkHost)
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
    properties.put(SolrInterpreter.ZK_HOST, zkHost)
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
