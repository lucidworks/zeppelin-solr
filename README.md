# Zeppelin Solr Interpreter

* Interpreter built with SolrCloud backend and allows user to issue Solr queries and display the results in Zeppelin UI

### Installation
1. Download and untar [Apache Zeppelin](https://zeppelin.apache.org/download.html) distribution if you don't have it already (Go lite or all)
2. Install this interpreter via command

```apple js
./bin/install-interpreter.sh --name zeppelin-solr --artifact com.lucidworks.zeppelin:zeppelin-solr:0.0.1-beta2
```

After running the above command

1. Restart Zeppelin
2. Create interpreter setting in 'Interpreter' menu on Zeppelin GUI
3. Create a notebook with the 'solr' interpreter

![create-settings](https://raw.githubusercontent.com/lucidworks/zeppelin-solr/master/images/create-interp-setting.png)

### Configuring the Interpreter
Set the config `solr.zkhost ` in the Solr Interpreter settings. This should point to the zkhost of SolrCloud cluster

### Commands list
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

### Example

![screenshot](http://i.imgur.com/DmNIj3T.png)
