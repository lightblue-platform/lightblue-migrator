lightblue-migrator
==================

#Packages

##core
This includes classes and configurations to manage feature flags for migrating data between one lightblue entity and another.  Much of this is a wrapper around Togglz functionality, with lightblue migration-specific features. See [How to configure togglz for migration to lightblue?](TOGGLZ.md).

See http://slideshare.net/derek63/lightblue-migration for more information on how to do a lightblue migration.

##web
This is a web application for managing the lightblue migration feature flags, and it pretty much the stock Togglz web application.

##migrator
The migrator and consistency checker builds as a self-contained jar using the Maven Shade plugin, and can be initiated from the command line using arguments.  An example of how to launch this batch application is included below. 

```shell
java -jar migrator-1.0.0-alldeps.jar 
--name checker_0
--host lightblue.io
--config=lightblue-client.properties
--configversion=1.0.0
--jobversion=1.0.0
--sourceconfig=source-lightblue-client.properties
--destinationconfig=destination-lightblue-client.properties
```

### How it works

The migrator/consistency checker expects these entities defined in the database:
 * MigrationConfiguration
 * MigrationJob
 * ActiveExecution
The default versions of these entities are used for migration/consistency checking.

Execution starts with determining the main configuration. The main
configuration gives the name of the instance of migrator running, and
configuration to connect to lightblue. Then, Main starts the
Controller thread using this configuration.

Controller thread is responsible for reading the migration
configuration from the database, and managing threads assigned for
this instance of migrator/consistency checker instance. For each
instance of the MigrationConfiguration in the database, the Controller
creates a MigratorController thread. Each thread periodically reads
the configuration database to get a recent copy of the migration
configuration. If the migration configuration is removed from the
database, the corresponding migrator thread terminates. The
controllers creates new migrator threads as new configuration items
are added.

MigratorController thread attempts to acquire a migration job. This is
done using a locking protocol to prevent two nodes choosing the same
job for processing. The algorithm is as follows:
 * The migration jobs whose scheduled dates have passed and available are loaded
   in batches (64 jobs at a time).
 * For each batch, one of the available jobs is picked randomly.
 * An ActiveExecution record is created in the database for the selected job. The ActiveExecution has
   a unique index on migration job id, so there can only be one record for each job. If another
   thread/node picks the same job, only one of the nodes will be able to create the record, and all
   others will fail.
 * If ActiveExecution creation is successful, thread acquires the job. It updates the job status to processing, and
   starts migration
 * If ActiveExecution creation fails, thread picks another job.

A word of caution: this procotol depends on the unique index on
ActiveExecution.migrationJobId. Uniqueness is not guaranteed in case
of a network partition. To prevent unexpected behavior, the collection
containing ActiveExecution should not be replicated.

Each migration job thread reads the entities using the query given in
the migration job, and attempts to migrate them to the
destination. The migration jobs should be created using queries that
partition the data set in similar sizes.

###[facade](facade)
The facade helps with migrating a service to lightblue.

###[entity-consistency-checker](entity-consistency-checker)
A utility to compare POJOs field by field. Use it during migration to lightblue to ensure consistency between entities returned by legacy and lightblue DAOs.

# License

The license of lightblue is [GPLv3](https://www.gnu.org/licenses/gpl.html).  See LICENSE in root of project for the full text.
