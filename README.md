lightblue-migrator
==================

###core
This includes classes and configurations to manage feature flags for migrating a legacy service to lightblue.  Much of this is a wrapper around Togglz functionality, with lightblue migration-specific features. 

See http://slideshare.net/derek63/lightblue-migration for more information on how to do a lightblue migration.

###web
This is a web application for managing the lightblue migration feature flags, and it pretty much the stock Togglz web application.

###consistency-checker
The consistency checker builds as a self-contained jar using the Maven Shade plugin, and can be initiated from the command line using arguments.  An example of how to use this batch application is included below. 

```shell
java -jar consistency-checker-1.0.0-alldeps.jar 
--lightblue-entity-name=country 
--lightblue-entity-version=1.0.0 
--legacy-entity-name=country-legacy 
--legacy-entity-version=1.0.0 
--lightblue-service-uri=http://demo.lightblue.io/rest/data 
--legacy-service-uri=http://demo.lightblue.io/rest/data 
--legacy-find-json='{"entity": "country","entityVersion": "1.0.0","query": {"field": "iso2code","op": "=","rvalue": "CA"},"projection": "*"}' 
--lightblue-find-json='{"entity": "country-legacy","entityVersion": "1.0.0","query": {"field": "iso2code","op": "=","rvalue": "CA"},"projection": "*"}' 
--lightblue-save-json='{"data"="$nodeData","upsert"=true,"projection"="*"}' --overwrite-lightblue=true
```