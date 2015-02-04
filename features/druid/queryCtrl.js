define([
  'angular',
  'lodash'
],
function (angular, _) {
  'use strict';

  var module = angular.module('grafana.controllers');

  module.controller('DruidTargetCtrl', function($scope, $q, $timeout, $log) {
    
    var
    validateGroupByQuery = function(target, errs) {
      if (!target.groupBy) {
        errs.groupBy = "Must list dimensions to group by.";
        return false;
      }
      if (!Array.isArray(target.groupBy)) {
        target.groupBy = target.groupBy.split(",");
      }
      return true;
    },
    validateTopNQuery = function(target, errs) {
      if (!target.threshold) {
        errs.threshold = "Must specify a threshold";
        return false;
      }
      var intThreshold = parseInt(target.threshold);
      if (isNaN(intThreshold)) {
        errs.threshold = "Threshold must be a integer";
        return false;
      }
      target.threshold = intThreshold
      if (!target.metric) {
        errs.metric = "Must specify a metric";
        return false;
      }
      if (!target.dimension) {
        errs.dimension = "Must specify a dimension";
        return false;
      }
      return true;
    },
    validateSelectorFilter = function(target) {
      if (!target.currentFilter.dimension) {
        return "Must provide dimension name for selector filter.";
      }
      if (!target.currentFilter.value) {
        //Empty string is how you match null or empty in Druid
        target.currentFilter.value = "";
      }
      return null;
    },
    validateRegexFilter = function(target) {
      if (!target.currentFilter.dimension) {
        return "Must provide dimension name for regex filter.";
      }
      if (!target.currentFilter.pattern) {
        return "Must provide pattern for regex filter.";
      }
      return null;
    },
    validateCountAggregator = function(target) {
      if (!target.currentAggregator.name) {
        return "Must provide an output name for count aggregator.";
      }
      return null;
    },
    validateSimpleAggregator = function(type, target) {
      if (!target.currentAggregator.name) {
        return "Must provide an output name for " + type + " aggregator.";
      }
      if (!target.currentAggregator.fieldName) {
        return "Must provide a metric name for " + type + " aggregator.";
      }
      //TODO - check that fieldName is a valid metric (exists and of correct type)
      return null;
    },
    validateApproxHistogramFoldAggregator = function(target) {
      var err = validateSimpleAggregator('approxHistogramFold', target);
      if (err) { return err; }
      //TODO - check that resolution and numBuckets are ints (if given)
      //TODO - check that lowerLimit and upperLimit are flots (if given)
      return null;
    },
    validateSimplePostAggregator = function(type, target) {
      if (!target.currentPostAggregator.name) {
        return "Must provide an output name for " + type + " post aggregator.";
      }
      if (!target.currentPostAggregator.fieldName) {
        return "Must provide an aggregator name for " + type + " post aggregator.";
      }
      //TODO - check that fieldName is a valid aggregation (exists and of correct type)
      return null;
    },
    validateQuantilePostAggregator = function (target) {
      var err = validateSimplePostAggregator('quantile', target);
      if (err) { return err; }
      if (!target.currentPostAggregator.probability) {
        return "Must provide a probability for the quantile post aggregator.";
      }
      return null;
    },
    validateArithmeticPostAggregator = function(target) {
      if (!target.currentPostAggregator.name) {
        return "Must provide an output name for arithmetic post aggregator.";
      }
      if (!target.currentPostAggregator.fn) {
        return "Must provide a function for arithmetic post aggregator.";
      }
      if (!isValidArithmeticPostAggregatorFn(target.currentPostAggregator.fn)) {
        return "Invalid arithmetic function";
      }
      if (!target.currentPostAggregator.fields) {
        return "Must provide a list of fields for arithmetic post aggregator.";
      }
      else {
        if (!Array.isArray(target.currentPostAggregator.fields)) {
          target.currentPostAggregator.fields = target.currentPostAggregator.fields.split(",").map(function(f){ return f.trim(); });
        }
        if (target.currentPostAggregator.fields.length < 2) {
          return "Must provide at least two fields for arithmetic post aggregator.";
        }
      }
      return null;
    },
    queryTypeValidators = {
      "timeseries": _.noop,
      "groupBy": validateGroupByQuery,
      "topN": validateTopNQuery
    },
    filterValidators = {
      "selector": validateSelectorFilter,
      "regex": validateRegexFilter
    },
    aggregatorValidators = {
      "count": validateCountAggregator,
      "longSum": _.partial(validateSimpleAggregator, 'longSum'),
      "doubleSum": _.partial(validateSimpleAggregator, 'doubleSum'),
      "approxHistogramFold": validateApproxHistogramFoldAggregator,
      "hyperUnique": _.partial(validateSimpleAggregator, 'hyperUnique')
    },
    postAggregatorValidators = {
      "arithmetic": validateArithmeticPostAggregator,
      "quantile": validateQuantilePostAggregator
    },
    arithmeticPostAggregatorFns = {'+': null, '-': null, '*': null, '/': null},
    defaultQueryType = "timeseries",
    defaultFilterType = "selector",
    defaultAggregatorType = "count",
    defaultPostAggregator = {type: 'arithmetic', 'fn': '+'},
    customGranularities = ['minute', 'fifteen_minute', 'thirty_minute', 'hour', 'day'],
    defaultCustomGranularity = 'minute';

    $scope.init = function() {
      $scope.target.errors = validateTarget($scope.target);
      $scope.queryTypes = _.keys(queryTypeValidators);
      $scope.filterTypes = _.keys(filterValidators);
      $scope.aggregatorTypes = _.keys(aggregatorValidators);
      $scope.postAggregatorTypes = _.keys(postAggregatorValidators);
      $scope.arithmeticPostAggregatorFns = _.keys(arithmeticPostAggregatorFns);
      $scope.customGranularities = customGranularities;

      if (!$scope.target.queryType) {
        $scope.target.queryType = defaultQueryType;
      }

      if (!$scope.target.currentFilter) {
        clearCurrentFilter();
      }

      if (!$scope.target.currentAggregator) {
        clearCurrentAggregator();
      }

      if (!$scope.target.currentPostAggregator) {
        clearCurrentPostAggregator();
      }

      if (!$scope.target.customGranularity) {
        $scope.target.customGranularity = defaultCustomGranularity;
      }

      $scope.$on('typeahead-updated', function() {
        $timeout($scope.targetBlur);
      });
    };

    /*
      rhoover: copied this function from OpenTSDB.
        I don't know what the comment below refers to
    */
    $scope.targetBlur = function() {
      $scope.target.errors = validateTarget($scope.target);

      // this does not work so good
      if (!_.isEqual($scope.oldTarget, $scope.target) && _.isEmpty($scope.target.errors)) {
        $scope.oldTarget = angular.copy($scope.target);
        $scope.get_data();
      }
    };

    $scope.duplicate = function() {
      var clone = angular.copy($scope.target);
      $scope.panel.targets.push(clone);
    };

    $scope.listDataSources = function(query, callback) {
      if (!$scope.dataSourceList) {
        return $scope.datasource.getDataSources().then(function(result) {
          $scope.dataSourceList = result.data;
          callback($scope.dataSourceList);
        });
      }
      else {
        return $scope.dataSourceList;
      }
    };

    $scope.getDimensions = function(query, callback) {
      $log.debug("Dimension type-ahead query for: " + query);
      return $scope.getDimensionsAndMetrics(query).then(function (dimsAndMetrics) {
        callback(dimsAndMetrics[0]);
      });
    };

    $scope.getDimensionsAndMetrics = function(query) {
      if (!$scope.dimensionsAndMetrics) {
        $log.debug("Fetch schame: no cached value to use");
        if (!$scope.dimensionsAndMetricsPromise) {
          $log.debug("Fetching schema from Druid");
          $scope.dimensionsAndMetricsPromise = $scope.datasource.getDimensionsAndMetrics($scope.target, $scope.range)
            .then(function(result) {
              $scope.dimensionsAndMetricsPromise = null;
              $scope.dimensionsAndMetrics = result;
              return $scope.dimensionsAndMetrics;
            });
        }
        else {
          $log.debug("Schema fetch already in progress...returning same promise");
        }
        return $scope.dimensionsAndMetricsPromise;
      }
      else {
        $log.debug("Using cached value for schema lookup");
        var deferred = $q.defer();
        deferred.resolve($scope.dimensionsAndMetrics);
        return deferred.promise;
      }
    };

    $scope.addFilter = function() {
      if (!$scope.addFilterMode) {
        //Enabling this mode will display the filter inputs
        $scope.addFilterMode = true;
        return;
      }

      if (!$scope.target.filters) {
        $scope.target.filters = [];
      }

      $scope.target.errors = validateTarget($scope.target);
      if (!$scope.target.errors.currentFilter) {
        //Add new filter to the list
        $scope.target.filters.push($scope.target.currentFilter);
        clearCurrentFilter();
        $scope.addFilterMode = false;
      }

      $scope.targetBlur();
    };

    $scope.removeFilter = function(index) {
      $scope.target.filters.splice(index, 1);
      $scope.targetBlur();
    };

    $scope.clearCurrentFilter = function() {
      clearCurrentFilter();
      $scope.addFilterMode = false;
      $scope.targetBlur();
    };

    $scope.addAggregator = function() {
      if (!$scope.addAggregatorMode) {
        $scope.addAggregatorMode = true;
        return;
      }

      if (!$scope.target.aggregators) {
        $scope.target.aggregators = [];
      }

      $scope.target.errors = validateTarget($scope.target);
      if (!$scope.target.errors.currentAggregator) {
        //Add new aggregator to the list
        $scope.target.aggregators.push($scope.target.currentAggregator);
        clearCurrentAggregator();
        $scope.addAggregatorMode = false;
      }

      $scope.targetBlur();
    };

    $scope.removeAggregator = function(index) {
      $scope.target.aggregators.splice(index, 1);
      $scope.targetBlur();
    };

    $scope.clearCurrentAggregator = function() {
      clearCurrentAggregator();
      $scope.addAggregatorMode = false;
      $scope.targetBlur();
    };

    $scope.addPostAggregator = function() {
      if (!$scope.addPostAggregatorMode) {
        $scope.addPostAggregatorMode = true;
        return;
      }

      if (!$scope.target.postAggregators) {
        $scope.target.postAggregators = [];
      }

      $scope.target.errors = validateTarget($scope.target);
      if (!$scope.target.errors.currentPostAggregator) {
        //Add new post aggregator to the list
        $scope.target.postAggregators.push($scope.target.currentPostAggregator);
        clearCurrentPostAggregator();
        $scope.addPostAggregatorMode = false;
      }

      $scope.targetBlur();
    };

    $scope.removePostAggregator = function(index) {
      $scope.target.postAggregators.splice(index, 1);
      $scope.targetBlur();
    };

    $scope.clearCurrentPostAggregator = function() {
      clearCurrentPostAggregator();
      $scope.addPostAggregatorMode = false;
      $scope.targetBlur();
    };

    function isValidFilterType(type) {
      return _.has(filterValidators, type);
    }

    function isValidAggregatorType(type) {
      return _.has(aggregatorValidators, type);
    }

    function isValidPostAggregatorType(type) {
      return _.has(postAggregatorValidators, type);
    }

    function isValidQueryType(type) {
      return _.has(queryTypeValidators, type);
    }

    function isValidArithmeticPostAggregatorFn(fn) {
      return _.has(arithmeticPostAggregatorFns, fn);
    }

    function clearCurrentFilter() {
      $scope.target.currentFilter = {type: defaultFilterType};
    }

    function clearCurrentAggregator() {
      $scope.target.currentAggregator = {type: defaultAggregatorType};
    }

    function clearCurrentPostAggregator() {
      $scope.target.currentPostAggregator = _.clone(defaultPostAggregator);
    }

    function validateTarget(target) {
      var validatorOut, errs = {};

      if (!target.datasource) {
        errs.datasource = "You must supply a datasource name.";
      }

      if (!target.queryType) {
        errs.queryType = "You must supply a query type.";
      }
      else if (!isValidQueryType(target.queryType)) {
        errs.queryType = "Unknown query type: " + target.queryType + ".";
      }
      else {
        queryTypeValidators[target.queryType](target, errs);
      }

      if (target.shouldOverrideGranularity) {
        if (target.customGranularity) {
          if (!_.contains(customGranularities, target.customGranularity)) {
            errs.customGranularity = "Invalid granularity.";
          }
        }
        else {
          errs.customGranularity = "You must choose a granularity.";
        }
      }

      if ($scope.addFilterMode) {
        if (!isValidFilterType(target.currentFilter.type)) {
          errs.currentFilter = "Invalid filter type: " + target.currentFilter.type + ".";
        }
        else {
          validatorOut = filterValidators[target.currentFilter.type](target);
          if (validatorOut) {
            errs.currentFilter = validatorOut;
          }
        }
      }

      if ($scope.addAggregatorMode) {
        if (!isValidAggregatorType(target.currentAggregator.type)) {
          errs.currentAggregator = "Invalid aggregator type: " + target.currentAggregator.type + ".";
        }
        else {
          validatorOut = aggregatorValidators[target.currentAggregator.type](target);
          if (validatorOut) {
            errs.currentAggregator = validatorOut;
          }
        }
      }

      if (!$scope.target.aggregators) {
        errs.aggregators = "You must supply at least one aggregator";
      }

      if ($scope.addPostAggregatorMode) {
        if (!isValidPostAggregatorType(target.currentPostAggregator.type)) {
          errs.currentPostAggregator = "Invalid post aggregator type: " + target.currentPostAggregator.type + ".";
        }
        else {
          validatorOut = postAggregatorValidators[target.currentPostAggregator.type](target);
          if (validatorOut) {
            errs.currentPostAggregator = validatorOut;
          }
        }
      }

      return errs;
    }

  });

});
