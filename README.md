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
![list collections](https://raw.githubusercontent.com/kiranchitturi/zeppelin-solr/master/images/collections-list.png)

#### use
Set a collection to use in the notebook. This has to be performed before any other commands
![use collection](https://raw.githubusercontent.com/kiranchitturi/zeppelin-solr/master/images/use-command.png)

#### search
![search command](https://raw.githubusercontent.com/kiranchitturi/zeppelin-solr/master/images/search-command.png)

### facet
![Facet command](https://raw.githubusercontent.com/kiranchitturi/zeppelin-solr/master/images/facet-command.png)

#### stream
![Stream command](https://raw.githubusercontent.com/kiranchitturi/zeppelin-solr/master/images/stream-command.png)

#### sql
![sql command](https://raw.githubusercontent.com/kiranchitturi/zeppelin-solr/master/images/sql-command.png)
