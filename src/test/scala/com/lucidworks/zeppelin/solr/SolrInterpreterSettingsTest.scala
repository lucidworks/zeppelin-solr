package com.lucidworks.zeppelin.solr

import java.util.Properties

import org.apache.solr.common.SolrException
import org.apache.zeppelin.interpreter.InterpreterResult


class SolrInterpreterSettingsTest extends TestSuiteBuilder {
  

  test("Unknown command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.BASE_URL, baseUrl)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    val result = solrInterpreter.interpret("index asdjkasd", null)
    assert(result.code().eq(InterpreterResult.Code.INCOMPLETE))
  }


}
