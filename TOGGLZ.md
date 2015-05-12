How to configure togglz for migration to lightblue?
==================================================

Classes from [core](core) module will help you with that.

### Setting up Togglz with cached jdbc backend

Keeping your feature flag configurations in a database provides an easy way to manipulate those configs for clustered applications. ```LightblueMigrationStateRepositoryProvider``` provides a convient way to initialize JDBCStateRepository from a property file. Example usage:

```java
featureManager = new FeatureManagerBuilder()
                .togglzConfig(new LightblueMigrationTogglzConfig(new LightblueMigrationStateRepositoryProvider("features.properties")))
                .build();
```
See [Togglz documentation for information on FeatureManagerProvider](http://www.togglz.org/documentation/advanced-config.html).

features.properties is a file on the classpath with following settings:
```
datasourceJndi=java:jboss/datasources/TestDS
tableName=TOGGLZ_TEST
cacheSeconds=180
noCommit=true
```
```noCommit``` should be set to true when app is deployed in JEE container which manages jdbc connections.

Feature flags table structure:
```sql
CREATE TABLE `TOGGLZ_TEST` (
  `FEATURE_NAME` varchar(100) NOT NULL,
  `FEATURE_ENABLED` int(11) DEFAULT NULL,
  `STRATEGY_ID` varchar(200) DEFAULT NULL,
  `STRATEGY_PARAMS` varchar(2000) DEFAULT NULL,
  PRIMARY KEY (`FEATURE_NAME`)
);
```

Initial migration phase:
```sql
INSERT INTO `TOGGLZ_TEST` VALUES ('CHECK_READ_CONSISTENCY',1,NULL,NULL);
INSERT INTO `TOGGLZ_TEST` VALUES ('CHECK_WRITE_CONSISTENCY',1,NULL,NULL);
INSERT INTO `TOGGLZ_TEST` VALUES ('READ_DESTINATION_ENTITY',1,'gradual','percentage=0');
INSERT INTO `TOGGLZ_TEST` VALUES ('READ_SOURCE_ENTITY',0,NULL,NULL);
INSERT INTO `TOGGLZ_TEST` VALUES ('WRITE_DESTINATION_ENTITY',1,'gradual','percentage=0');
INSERT INTO `TOGGLZ_TEST` VALUES ('WRITE_SOURCE_ENTITY',1,NULL,NULL);
```

Example: To start writing to lightblue (destination) at 15%, you need to run following update:
```sql
UPDATE `TOGGLZ_TEST` SET STRATEGY_PARAMS='percentage=15' WHERE FEATURE_NAME='WRITE_DESTINATION_ENTITY';
```

### Using gradual activation strategy to control load

Feature flags are designed to control features, not load balancing. However, with some tweaking, it's possible to use the Togglz' gradual activation strategy for this purpose. The tweak is to generate username for each call to DAOFacade (```TogglzRandomUsername.init()```). For information on DAOFacade and it's role during migration see [utils](utils).
