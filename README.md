Elasticsearch Event Hook Pugin
=======================

## Overview

Event Hook Plugin provides a feature to invoke your script on Elasticsearch Cluster event.

## Version

| Version   | Elasticsearch |
|:---------:|:-------------:|
| master    | 1.4.X         |
| 1.4.0     | 1.4.0.Beta1   |
| 1.3.0     | 1.3.2         |

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-eventhook/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

### Install DynaRank Plugin

    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-eventhook/1.4.0

## Getting Started

Event Hook Plugin invokes your scripts in .eventhook index.
A type of .eventhook index is an event name.
A script has a type "all" is invoked on all events.
Event Hook Plugin uses Elasticsearch script service.

### Print All Events

    curl -s -XPOST localhost:9200/.eventhook/all/print_event?pretty -d '{
      "priority": 1,
      "lang": "groovy",
      "script": "logger.info(\"[\"+cluster.getLocalNode().name()+\"]:\"+eventType+\" => \"+event.source())",
      "script_type": "inline"
    }'

### Disable Allocation On Less Than 3 Nodes

The following script is for disappearing a master node:

    curl -s -XPOST localhost:9200/.eventhook/routing_table_updater/allocation_disable_on_master?pretty -d '{
      "priority": 1,
      "lang": "groovy",
      "script": "if(nodes.nodeInfo().length<3)cluster.setTransientSettings(\"cluster.routing.allocation.enable\",\"none\")",
      "script_type": "inline"
    }'

For disappearing a non-master node:

    curl -s -XPOST localhost:9200/.eventhook/zen_disco_node_left/allocation_disable_on_nonmaster?pretty -d '{
      "priority": 1,
      "lang": "groovy",
      "script": "if(nodes.nodeInfo().length<3)cluster.setTransientSettings(\"cluster.routing.allocation.enable\",\"none\")",
      "script_type": "inline"
    }'

## Specification

### Index Mapping

| Name        | Type   | Description |
|:------------|:-------|:-----|
| priority    | long   | a sort order to execute a script. 1 is a high priority. |
| lang        | string | a script language. ex. groovy, native, mvel...          |
| script      | string | a script.                                               |
| script_type | string | a script type. ex. inline, indexded, file.              |



