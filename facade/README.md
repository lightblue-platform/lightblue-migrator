# Lightblue-migrator-facade

The facade helps with migrating a service to lightblue by offering following features:
* phased migration using feature flag support (Togglz - see LightblueMigrationFeatures),
* parallel processing (when possible),
* error handling and timeouts (destination too slow to respond) transparent to the service client,
* passing generated data between source and destination (Lightblue) services (e.g. identifiers, dates, password salts, etc.),
* checking for data integrity across source and destination (Lightblue) entities and logging data inconsistencies and
* limited functionality to prevent secrets being logged.

The facade stands in front of the source and destination services, directing traffic as needed. It supports Read and Write operations controlled independently (e.g. 100% writes go to Lightblue but only 25% reads).

## Initializing dynamic proxy for the facade
```java
CountryDAO countryDAOFacade = FacadeProxyFactory.createFacadeProxy(legacyCountryDAO, lightblueCountryDAO, CountryDAO.class);
```

The dynamic proxy directs DAO interface api calls to correct DAOFacadeBase methods based on annotations on the DAO interface apis. See [CountryDAO](src/test/java/com/redhat/lightblue/migrator/facade/CountryDAO.java) and [FacadeProxyFactory](src/main/java/com/redhat/lightblue/migrator/facade/proxy/FacadeProxyFactory.java) for more information on how to use the annotations.

### Parallel operations and passing generated data

Some operations, usually writes, generate data like identifiers, dates, password salts, etc. Since generated data needs to be the same in both source and destination (Lightblue) services and data stores, it needs to be shared between services. This is what [SharedStore](src/main/java/com/redhat/lightblue/migrator/facade/sharedstore/SharedStore.java) is for. Passing shared data is only possible for serial operations (e.g. @WriteOperation(parallel=false), which is the default) to ensure that source service generates all data before the destination (Lightblue) service starts consuming it.

### Timeout configuration

Following configuration:
```
com.redhat.lightblue.migrator.facade.timeout.CountryDAO=2000
  com.redhat.lightblue.migrator.facade.timeout.CountryDAO.getCountry=5000
```
means that facade will wait up to 2s for getCountry method on destination (Lightblue) service to finish execution and 2s for any other method. Timeouts make sense only when
source service is being called in addition to the destination service, so that source service response can be returned when destination takes too long to respond.

## [How to configure togglz for migration to lightblue?](TOGGLZ.md)

## Examples

See unit tests.
