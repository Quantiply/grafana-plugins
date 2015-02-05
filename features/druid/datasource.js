define([
  'angular',
  'lodash',
  'kbn',
  'moment',
  './queryCtrl',
],
function (angular, _, kbn, moment) {
  'use strict';

  var module = angular.module('grafana.services');

  module.factory('DruidDatasource', function($q, $http, templateSrv, $timeout) {

    function replaceTemplateValues(obj, attrList) {
      var substitutedVals = attrList.map(function (attr) {
        return templateSrv.replace(obj[attr]);
      });
      return _.assign(_.clone(obj, true), _.zipObject(attrList, substitutedVals));
    }

    var filterTemplateExpanders = {
      "selector": _.partialRight(replaceTemplateValues, ['value']),
      "regex": _.partialRight(replaceTemplateValues, ['pattern']),
    };

    function DruidDatasource(datasource) {
      this.type = 'druid';
      this.editorSrc = 'plugins/features/druid/partials/query.editor.html';
      this.url = datasource.url;
      this.name = datasource.name;
      this.supportMetrics = true;
    }

    //Get list of available datasources
    DruidDatasource.prototype.getDataSources = function() {
      return $http({method: 'GET', url: this.url + '/datasources'}).then(function (response) {
        return response.data;
      });
    }

    /* Returns a promise which returns
      {"dimensions":["page_url","ip_netspeed", ...],"metrics":["count", ...]}
    */
    DruidDatasource.prototype.getDimensionsAndMetrics = function (target, range) {
      var datasource = target.datasource;
      return $http({method: 'GET', url: this.url + '/datasources/' + datasource}).then(function (response) {
        return response.data;
      });
    }

    //Get segment metadata
    DruidDatasource.prototype.getSchema = function (target, range) {
      var dataSourceObj = this;
      var datasource = target.datasource;
      //Using the most recent segment for metadata
      var to = dateToMoment(range.to);
      var from = to.subtract(1, 's');
      return dataSourceObj._getSchema(datasource, from, to);
    }

    DruidDatasource.prototype._getSchema = function (datasource, from, to) {
      var query = {
        "queryType": "segmentMetadata",
        "dataSource": datasource,
        "intervals": getQueryIntervals(from, to),
        "merge": true
      };
      return this._druidQuery(query);
    };

    // Called once per panel (graph)
    DruidDatasource.prototype.query = function(options) {
      var dataSource = this;
      var from = dateToMoment(options.range.from);
      var to = dateToMoment(options.range.to);

      console.log(options);

      var promises = options.targets.map(function (target) {
        var granularity = target.shouldOverrideGranularity? target.customGranularity : intervalToGranularity(options.interval);
        return dataSource._doQuery(from, to, granularity, target);
      });

      return $q.all(promises).then(function(results) {
        return { data: _.flatten(results) };
      });
    };

    DruidDatasource.prototype._doQuery = function (from, to, granularity, target) {
      var datasource = target.datasource;
      var filters = target.filters;
      var aggregators = target.aggregators;
      var postAggregators = target.postAggregators;
      var groupBy = target.groupBy;
      var limitSpec = null;
      var metricNames = getMetricNames(aggregators, postAggregators);

      if (target.queryType === 'topN') {
        var threshold = target.threshold;
        var metric = target.metric;
        var dimension = target.dimension;
        return this._topNQuery(datasource, from, to, granularity, filters, aggregators, postAggregators, threshold, metric, dimension)
          .then(function(response) {
            return convertTopNData(response.data, dimension, metric);
          });
      }

      if (target.queryType === 'groupBy') {
        if (target.hasLimit) {
          limitSpec = getLimitSpec(target.limit, target.orderBy); 
        }
        return this._groupByQuery(datasource, from, to, granularity, filters, aggregators, postAggregators, groupBy, limitSpec)
          .then(function(response) {
            return convertGroupByData(response.data, groupBy, metricNames);
          });
      }

      return this._timeSeriesQuery(datasource, from, to, granularity, filters, aggregators, postAggregators)
        .then(function(response) {
          return convertTimeSeriesData(response.data, metricNames);
        });
    };

    DruidDatasource.prototype._timeSeriesQuery = function (datasource, from, to, granularity, filters, aggregators, postAggregators) {
      var query = {
        "queryType": "timeseries",
        "dataSource": datasource,
        "granularity": granularity,
        "aggregations": aggregators,
        "postAggregations": postAggregators,
        "intervals": getQueryIntervals(from, to)
      };

      if (filters && filters.length > 0) {
        query.filter = buildFilterTree(filters);
      }

      return this._druidQuery(query);
    };

    DruidDatasource.prototype._topNQuery = function (datasource, from, to, granularity, filters, aggregators, postAggregators, threshold, metric, dimension) {
      var query = {
        "queryType": "topN",
        "dataSource": datasource,
        "granularity": granularity,
        "threshold": threshold,
        "dimension": dimension,
        "metric": metric,
        "aggregations": aggregators,
        "postAggregations": postAggregators,
        "intervals": getQueryIntervals(from, to)
      };

      if (filters && filters.length > 0) {
        query.filter = buildFilterTree(filters);
      }

      return this._druidQuery(query);
    };

    DruidDatasource.prototype._groupByQuery = function (datasource, from, to, granularity, filters, aggregators, postAggregators, groupBy, limitSpec) {
      var query = {
        "queryType": "groupBy",
        "dataSource": datasource,
        "granularity": granularity,
        "dimensions": groupBy,
        "aggregations": aggregators,
        "postAggregations": postAggregators,
        "intervals": getQueryIntervals(from, to),
        "limitSpec": limitSpec
      };

      if (filters && filters.length > 0) {
        query.filter = buildFilterTree(filters);
      }

      return this._druidQuery(query);
    };

    DruidDatasource.prototype._druidQuery = function (query) {
      var options = {
        method: 'POST',
        url: this.url,
        data: query
      };
      console.log(query);
      return $http(options);
    };

    function getLimitSpec(limitNum, orderBy) {
      return {
        "type": "default",
        "limit": limitNum,
        "columns": orderBy.map(function (col) {
          return {"dimension": col, "direction": "DESCENDING"};
        })
      };
    }

    function buildFilterTree(filters) {
      //Do template variable replacement
      var replacedFilters = filters.map(function (filter) {
        return filterTemplateExpanders[filter.type](filter);
      })
      .map(function (filter) {
        var finalFilter = _.omit(filter, 'negate');
        if (filter.negate) {
          return { "type": "not", "field": finalFilter };
        }
        return finalFilter;
      });
      if (replacedFilters) {
        if (replacedFilters.length === 1) {
          return replacedFilters[0];
        }
        return  {
          "type": "and",
          "fields": replacedFilters
        };
      }
      return null;
    }

    function getQueryIntervals(from, to) {
      return [from.toISOString() + '/' + to.toISOString()];
    }

    function getMetricNames(aggregators, postAggregators) {
      var displayAggs = _.filter(aggregators, function (agg) {
        return agg.type !== 'approxHistogramFold';
      });
      return _.union(_.pluck(displayAggs, 'name'), _.pluck(postAggregators, 'name'));
    }

    function formatTimestamp(ts) {
      return moment(ts).format('X')*1000
    }

    function convertTimeSeriesData(md, metrics) {
      return metrics.map(function (metric) {
        return {
          target: metric,
          datapoints: md.map(function (item) {
            return [
              item.result[metric],
              formatTimestamp(item.timestamp)
            ];
          })
        };
      });
    }

    function getGroupName(groupBy, metric) {
      return groupBy.map(function (dim) {
        return metric.event[dim];
      })
      .join("-");
    }

    function convertTopNData(md, dimension, metric) {
      var mergedData = md.map(function (item) {
        /*
          Druid topN results look like this:
            [
              {
                "timestamp": "ts1",
                "result": [
                  {"<dim>": d1, "<metric>": mv1},
                  {"<dim>": d2, "<metric>": mv2}
                ]
              },
              {
                "timestamp": "ts2",
                "result": [
                  {"<dim>": d1, "<metric>": mv3},
                  {"<dim>": d2, "<metric>": mv4}
                ]
              },
              ...
            ]
        
          This first map() transforms this into a list of objects
          where the keys are dimension values
          and the values are [metricValue, unixTime] so that we get this:
            [
              {
                "d1": [mv1, ts1],
                "d2": [mv2, ts1]
              },
              {
                "d1": [mv3, ts2],
                "d2": [mv4, ts2]
              },
              ...
            ]        
        */
        var timestamp = formatTimestamp(item.timestamp);
        var keys = _.pluck(item.result, dimension);
        var vals = _.pluck(item.result, metric).map(function (val) { return [val, timestamp]});
        return _.zipObject(keys, vals);
      })
      .reduce(function (prev, curr) {
        /*
          Reduce() collapses all of the mapped objects into a single
          object.  The keys are dimension values
          and the values are arrays of all the values for the same key.
          The _.assign() function merges objects together and it's callback
          gets invoked for every key,value pair in the source (2nd argument).
          Since our initial value for reduce() is an empty object,
          the _.assign() callback will get called for every new val
          that we add to the final object.
        */
        return _.assign(prev, curr, function (pVal, cVal) {
          if (pVal) {
            pVal.push(cVal);
            return pVal;
          }
          return [cVal];
        });
      }, {});

      return _.map(mergedData, function (vals, key) {
        /*
          Second map converts the aggregated object into an array
        */
        return {
          target: key,
          datapoints: vals
        };
      });
    }

    function convertGroupByData(md, groupBy, metrics) {
      var mergedData = md.map(function (item) {
        /*
          The first map() transforms the list Druid events into a list of objects
          with keys of the form "<groupName>:<metric>" and values
          of the form [metricValue, unixTime]
        */
        var groupName = getGroupName(groupBy, item);
        var keys = metrics.map(function (metric) {
          return groupName + ":" + metric;
        });
        var vals = metrics.map(function (metric) {
          return [
            item.event[metric],
            formatTimestamp(item.timestamp)
          ];
        });
        return _.zipObject(keys, vals);
      })
      .reduce(function (prev, curr) {
        /*
          Reduce() collapses all of the mapped objects into a single
          object.  The keys are still of the form "<groupName>:<metric>"
          and the values are arrays of all the values for the same key.
          The _.assign() function merges objects together and it's callback
          gets invoked for every key,value pair in the source (2nd argument).
          Since our initial value for reduce() is an empty object,
          the _.assign() callback will get called for every new val
          that we add to the final object.
        */
        return _.assign(prev, curr, function (pVal, cVal) {
          if (pVal) {
            pVal.push(cVal);
            return pVal;
          }
          return [cVal];
        });
      }, {});

      return _.map(mergedData, function (vals, key) {
        /*
          Second map converts the aggregated object into an array
        */
        return {
          target: key,
          datapoints: vals
        };
      });
    }

    function dateToMoment(date) {
      if (date === 'now') {
        return moment();
      }
      return moment(kbn.parseDate(date));
    }

    function intervalToGranularity(kbnInterval) {
      var seconds = kbn.interval_to_seconds(kbnInterval);
      var duration = moment.duration(seconds, 'seconds');

      //Granularity none is too slow
      // if (duration.asMinutes() < 1.0) {
      //   return 'none';
      // }
      if (duration.asMinutes() < 15.0) {
        return 'minute';
      }
      if (duration.asMinutes() < 30.0) {
        return 'fifteen_minute';
      }
      if (duration.asHours() < 1.0) {
        return 'thirty_minute';
      }
      if (duration.asDays() < 1.0) {
        return 'hour';
      }
      return 'day';
    }

    return DruidDatasource;
  });

});
