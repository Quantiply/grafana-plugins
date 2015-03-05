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

  module.factory('DruidDatasource', function($q, $http, templateSrv, $timeout, $log) {

    function replaceTemplateValues(obj, attrList) {
      var substitutedVals = attrList.map(function (attr) {
        return templateSrv.replace(obj[attr]);
      });
      return _.assign(_.clone(obj, true), _.zipObject(attrList, substitutedVals));
    }

    var GRANULARITIES = [
      ['minute', moment.duration(1, 'minute')],
      ['fifteen_minute', moment.duration(15, 'minute')],
      ['thirty_minute', moment.duration(30, 'minute')],
      ['hour', moment.duration(1, 'hour')],
      ['day', moment.duration(1, 'day')]
    ];

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
    
    // Called once per panel (graph)
    DruidDatasource.prototype.query = function(options) {
      var dataSource = this;
      //Add 1 second or Grafana will not display the first point
      var from = dateToMoment(options.range.from).add(1, 'seconds');
      var to = dateToMoment(options.range.to);

      $log.debug(options);

      var promises = options.targets.map(function (target) {
        var maxDataPointsByResolution = options.maxDataPoints;
        var maxDataPointsByConfig = target.maxDataPoints? target.maxDataPoints : Number.MAX_VALUE;
        var maxDataPoints = Math.min(maxDataPointsByResolution, maxDataPointsByConfig);
        var granularity = target.shouldOverrideGranularity? target.customGranularity : computeGranularity(from, to, maxDataPoints);
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
      var intervals = getQueryIntervals(from, to);
      var promise = null;

      if (target.queryType === 'topN') {
        var threshold = target.limit;
        var metric = target.metric;
        var dimension = target.dimension;
        promise = this._topNQuery(datasource, intervals, granularity, filters, aggregators, postAggregators, threshold, metric, dimension)
          .then(function(response) {
            return convertTopNData(response.data, dimension, metric);
          });
      }
      else if (target.queryType === 'groupBy') {
        if (target.hasLimit) {
          limitSpec = getLimitSpec(target.limit, target.orderBy); 
        }
        promise = this._groupByQuery(datasource, intervals, granularity, filters, aggregators, postAggregators, groupBy, limitSpec)
          .then(function(response) {
            return convertGroupByData(response.data, groupBy, metricNames);
          });
      }
      else {
        promise = this._timeSeriesQuery(datasource, intervals, granularity, filters, aggregators, postAggregators)
          .then(function(response) {
            return convertTimeSeriesData(response.data, metricNames);
          });
      }
      /*
        At this point the promise will return an list of time series of this form
      [
        {
          target: <metric name>,
          datapoints: [
            [<metric value>, <timestamp in ms>],
            ...
          ]
        },
        ...
      ]
      
      Druid calculates metrics based on the intervals specified in the query but returns a timestamp rounded down.
      We need to adjust the first timestamp in each time series
      */
      return promise.then(function (metrics) {
        var fromMs = formatTimestamp(from);
        metrics.forEach(function (metric) {
          if (metric.datapoints[0][1] < fromMs) {
            metric.datapoints[0][1] = fromMs;
          }
        });
        return metrics;
      });
    };

    DruidDatasource.prototype._timeSeriesQuery = function (datasource, intervals, granularity, filters, aggregators, postAggregators) {
      var query = {
        "queryType": "timeseries",
        "dataSource": datasource,
        "granularity": granularity,
        "aggregations": aggregators,
        "postAggregations": postAggregators,
        "intervals": intervals
      };

      if (filters && filters.length > 0) {
        query.filter = buildFilterTree(filters);
      }

      return this._druidQuery(query);
    };

    DruidDatasource.prototype._topNQuery = function (datasource, intervals, granularity, filters, aggregators, postAggregators, threshold, metric, dimension) {
      var query = {
        "queryType": "topN",
        "dataSource": datasource,
        "granularity": granularity,
        "threshold": threshold,
        "dimension": dimension,
        "metric": metric,
        // "metric": {type: "inverted", metric: metric},
        "aggregations": aggregators,
        "postAggregations": postAggregators,
        "intervals": intervals
      };

      if (filters && filters.length > 0) {
        query.filter = buildFilterTree(filters);
      }

      return this._druidQuery(query);
    };

    DruidDatasource.prototype._groupByQuery = function (datasource, intervals, granularity, filters, aggregators, postAggregators, groupBy, limitSpec) {
      var query = {
        "queryType": "groupBy",
        "dataSource": datasource,
        "granularity": granularity,
        "dimensions": groupBy,
        "aggregations": aggregators,
        "postAggregations": postAggregators,
        "intervals": intervals,
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
      $log.debug(query);
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

    function getPostAggregators(target) {
      //roger
      
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

    function computeGranularity(from, to, maxDataPoints) {
      var intervalSecs = to.unix() - from.unix();
      /*
        Find the smallest granularity for which there
        will be fewer than maxDataPoints
      */
      var granularityEntry = _.find(GRANULARITIES, function(gEntry) {
        return Math.ceil(intervalSecs/gEntry[1].asSeconds()) <= maxDataPoints;
      });
      
      $log.debug("Calculated \"" + granularityEntry[0]  +  "\" granularity [" + Math.ceil(intervalSecs/granularityEntry[1].asSeconds()) + " pts]" + " for " + (intervalSecs/60).toFixed(0) + " minutes and max of " + maxDataPoints + " data points");
      return granularityEntry[0];
    }

    return DruidDatasource;
  });

});
