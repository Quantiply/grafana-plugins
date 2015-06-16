Grafana plugin for [Druid](http://druid.io/) real-time OLAP database. 

![Alt text](/../screenshot/features/druid/Unqiue.png?raw=true) 

## Status

This plugin is experimental.  It's usable but still needs TLC.  In particular, auto-completion for dimension names would really help. It supports timeseries, group by, and topN queries.  For the filters, it supports a list of filters (AND) and negation (NOT) on a single expression.  OR filters are not yet supported.  To completely support all filters, the editor will need to let you build a tree.

This plugin works with Grafana 1.9.x and has been tested against 1.9.1.  Note that from 1.8.x to 1.9.x, the timestamp format for Grafana was changed from seconds to milliseconds.

The code in Grafana master branch was recently reorganized so that all the files for each datasource live in a subdirectory under the [features](https://github.com/grafana/grafana/tree/master/src/app/features) directory.  We've followed that convention for this plugin.

## Installation

It must be installed in plugins/features/druid. You can do this by creating a symbolic link in the Grafana plugins directory so that the plugin/features directory maps to the features directory of this repo.

Add this to your Grafana configuration file.

```
      datasources: {
        druid: {
          type: 'DruidDatasource',
          url: '<druid broker url>'
        }
      },

      plugins: {
        panels: [],
        dependencies: ['features/druid/datasource'],
      }
```

An example configuration and dashboard is [here](https://github.com/Quantiply/grafana-druid-wikipedia/).