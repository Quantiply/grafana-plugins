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

  module.factory('DruidDatasource', function($q, $http, templateSrv) {

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
      this.editorSrc = 'app/features/druid/partials/query.editor.html';
      this.url = datasource.url;
      this.name = datasource.name;
      this.supportMetrics = true;
    }

    //Get list of available datasources
    DruidDatasource.prototype.getDataSources = function() {
      return $http({method: 'GET', url: this.url + '/datasources'});
    }

    // Called once per panel (graph)
    DruidDatasource.prototype.query = function(options) {
      var dataSource = this;
      var from = dateToMoment(options.range.from);
      var to = dateToMoment(options.range.to);

      console.log(options);

      var promises = options.targets.map(function (target) {
        var granularityScaleFactor = target.shouldOverrideGranularity? target.scaleGranularity : null;
        var granularity = intervalToGranularity(options.interval, granularityScaleFactor);
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
      var metricNames = getMetricNames(aggregators, postAggregators);

      if (target.queryType === 'groupBy') {
        return this._groupByQuery(datasource, from, to, granularity, filters, aggregators, postAggregators, groupBy)
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

    DruidDatasource.prototype._groupByQuery = function (datasource, from, to, granularity, filters, aggregators, postAggregators, groupBy) {
      var query = {
        "queryType": "groupBy",
        "dataSource": datasource,
        "granularity": granularity,
        "dimensions": groupBy,
        "aggregations": aggregators,
        "postAggregations": postAggregators,
        "intervals": getQueryIntervals(from, to)
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

    function buildFilterTree(filters) {
      //Do template variable replacement
      var replacedFilters = filters.map(function (filter) {
        return filterTemplateExpanders[filter.type](filter);
      });
      if (replacedFilters) {
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

    function convertTimeSeriesData(md, metrics) {
      return metrics.map(function (metric) {
        return {
          target: metric,
          datapoints: md.map(function (item) {
            return [
              item.result[metric],
              moment(item.timestamp).format('X')
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

    function convertGroupByData(md, groupBy, metrics) {
      var mergedData = md.map(function (item) {
        /*
          The first map() transforms each Druid event into an object
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
            moment(item.timestamp).format('X')
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

    function intervalToGranularity(kbnInterval, granularityScaleFactor) {
      var seconds = kbn.interval_to_seconds(kbnInterval);
      if (granularityScaleFactor) {
        seconds = seconds * granularityScaleFactor;
      }
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
