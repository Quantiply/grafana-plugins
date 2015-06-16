#Note: The Druid plugin is in [features/druid](features/druid)

grafana-plugins
===============

Extensions, custom &amp; experimental panels

Examples
===========

### Adding Custom Panels

~~~
plugins: {
  // list of plugin panels
  panels: {
   'custom panel name': { path: '../plugins/panels/custom.panel.example' } 
  },
  // requirejs modules in plugins folder that should be loaded
  // for example custom datasources
  dependencies: [],
}
~~~

### Adding custom data sources

~~~
datasources: {
  custom: {
    type: 'CustomDatasource',
    hello: 'some property'
  },
},

plugins: {
  dependencies: ['datasource.example']
},
~~~

