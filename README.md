# Zeppelin Solr Interpreter

* Interpreter built with SolrCloud backend and allows user to issue Solr queries and display the results in Zeppelin UI
* List of commands:
  1. list  
  2. use {collection}
  3. search {query}
  4. facet {query}
  5. stream {expr}
  6. sql {SQL statement}

### Configuring the Interpreter
Set the config `solr.zkhost ` in the Solr Interpreter settings. This should point to the zkhost of SolrCloud cluster


#### list
List the collections in the SolrCloud
Usage: `list`

#### use
Set a collection to use in the notebook. Displays the defined fields that have data with their type
Usage: `use {collection_name}`

#### search
Issue a search query and have the results displayed as table
Usage: `search {query-params}`

#### facet
Issue a query with facet fields and display the facet counts
Usage: `search {query-params}`

#### stream
Issue a streaming expression query and display the output as a table
Usage: `stream {stream-expr}`

#### sql
Issue an Solr SQL query and display the results as a table
Usage: `sql {sql-string}`

Example with all possible commands typed out:

![screenshot](http://i.imgur.com/DmNIj3T.png)
