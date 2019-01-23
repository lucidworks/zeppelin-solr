# Zeppelin Solr Interpreter

* Interpreter built with Apache Solr as backend and allows user to issue Solr queries and display results in the Zeppelin UI

### Installation
1. Download and untar [Apache Zeppelin](https://zeppelin.apache.org/download.html) distribution if you don't have it already (Go lite or all)
2. Install this interpreter via command

```apple js
./bin/install-interpreter.sh --name solr --artifact com.lucidworks.zeppelin:zeppelin-solr:0.1.1
```

After running the above command

1. Restart Zeppelin
2. Create interpreter setting in 'Interpreter' menu on Zeppelin GUI
3. Configure the existing 'solr' interpreter to point to zkhost of SolrCloud

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
Issue a search query and have the results displayed as table, collection param can be passed in here avoiding `use {collection_name}`

Usage: `search {query-params}`

#### facet
Issue a query with facet fields and display the facet counts. No need to explicitly add `facet=true` for these queries, collection param can be passed in here avoiding `use {collection_name}`

Usage: `facet {facet-params}`

#### stream
Issue a streaming expression query and display the output as a table

Usage: `stream {stream-expr}`

#### sql
Issue an Solr SQL query and display the results as a table

Usage: `sql {sql-string}`

### Troubleshooting 

* Check Solr interpreter log for any Solr errors (logs/zeppelin-interpreter-solr-\*) (Fixed in 0.1.1)
* Zeppelin 0.8.0 does not work well if interpreter does not have the same name as the interpreter installed. Not sure what is causing this. I would recommend using `solr` for interpreter name

### Example

![screenshot](http://i.imgur.com/DmNIj3T.png)


### Setting up Intellij IDEA for this project

1. clone the project to your local box
2. Make sure scala plugin is enabled for IntelliJ
3. In IntelliJ, click on 'File -> New -> Project from Existing Sources -> (Navigate to zeppelin-solr dir) -> **select pom.xml** -> click Open' 
4. Go through the steps for creating the project
5. Overwrite `.idea` project if the IDE prompts
6. Once deps are resolved, click on `Build Project` to verify

