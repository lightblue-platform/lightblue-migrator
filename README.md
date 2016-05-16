lightblue-migrator
==================

#Packages

##core
This includes classes and configurations to manage feature flags for migrating data between one lightblue entity and another.  Much of this is a wrapper around Togglz functionality, with lightblue migration-specific features. See [How to configure togglz for migration to lightblue?](TOGGLZ.md).

See http://slideshare.net/derek63/lightblue-migration for more information on how to do a lightblue migration.

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
### Migration data:

#### MigrationConfiguration:

There is a migration configuration entry for every migrated
entity. The default migration implementation reads entities from the
source lightblue instance, and writes them to the destination
lightblue instance. The MigrationConiguration.migratorClass entry can
specify a migrator implementation that can modify this behavior.

The migration configuration also specifies the consistency checker
class. The consistency checker implementation is invoked only if a
period is given. The consistency checker implementation wakes up
periodically based on the period, and creates new migration jobs for
the entries recently modified. It is assumed that the entities whose
consistency is a concern have timestamp fields. The consistency
checker creates migration jobs with queries that start from
'timestampInitialValue', including 'period' amount of time. Once the
migration jobs are created, it updates the 'timestampInitialValue' to
the latest query end value. This way, the next time the consistency
checker runs, it creates migration jobs for entries created or
modified since the latest time the last run checked. The actual
consistency checking and correcting/logging is done by the migration
jobs.

#### MigrationJob

Each migration job represents a reasonable amount of migration
data. The migration records should be partitioned in small batches to
improve better distribution, and better observability.

Migration jobs created by the consistency checker have
generated:true. Migration jobs created for the first migration must
have generated:false.

When created, a migration job should have status:available. The
migration jobs wait until 'scheduledDate' passes. For immediate
execution, set 'scheduledDate' to now, or earlier.

When migration job starts processing, the status is set to
'active'. If the migration job completes without any errors, the
status is set to 'completed'. If the migration fails, the status is
set to 'failed'. Once the cause of the failure is corrected, the job
can be rescheduled by setting status:available.

The failure information for every execution is also stored in the
migration job.

Migration jobs created by consistency checker has
MigrationJob.consistencyChecker fields populated. These are used to
prevent creation of duplicate jobs for the same range.

### How it works


The migrator/consistency checker expects these entities defined in the database:
 * MigrationConfiguration
 * MigrationJob

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

#### MigratorController

MigratorController thread attempts to acquire a migration job. This is
done using a locking protocol to prevent two nodes choosing the same
job for processing. The algorithm is as follows:
 * The migration jobs whose scheduled dates have passed and available are loaded
   in batches (64 jobs at a time).
 * For each batch, one of the available jobs is picked randomly and locked.
 * If locking is successful, thread acquires the job. It updates the job status to processing, and
   starts migration
 * If locking fails, thread picks another job.

Each migration job thread reads the entities using the query given in
the migration job, and attempts to migrate them to the
destination. The migration jobs should be created using queries that
partition the data set in similar sizes.

#### ConsistencyChecker

For each MigrationConfiguration with a non-null period, a
ConsistencyCheckerController thread is created. This thread runs once
at every MigrationConfiguration.period, and created migration jobs for
a range that is modified. The
ConsistencyCheckerController.createJobs() method is responsible for
creating migration jobs for a give date range. The default
implementation creates a migration job with a query returned by
Migrator.createRangeQuery implementation for the migrator for the
entity. This default behavior can be overridden by a different
consistency checker implementation as defined in
MigrationConfiguration.


### Cleanup

There is a CleanupThread that removes generated and completed
migration jobs from the database after about a week. The same thread
also changes the status jobs that are in active state for a long time
(default is 30 minutes) back to available, so they can be retried.

### ThreadMonitor

The ThreadMonitor expect pings from running migrator threads, and if a
migrator thread doesn't ping for a while (default 10 minutes), it
tries to interrupt the thread. If the thread doesn't response, the
thread is abandoned after another 10 minutes. The cleanup thread later
reschedules any incomplete jobs


###[facade](facade)
The facade helps with migrating a service to lightblue.

###[entity-consistency-checker](entity-consistency-checker)
A utility to compare POJOs field by field. Use it during migration to lightblue to ensure consistency between entities returned by legacy and lightblue DAOs.

# License

The license of lightblue is [GPLv3](https://www.gnu.org/licenses/gpl.html).  See LICENSE in root of project for the full text.
