# Lightblue-migrator-facade

The facade helps with migrating a service to lightblue by offering following features:
* phased migration using feature flag support (Togglz - see LightblueMigrationFeatures),
* parallel processing (when possible),
* error handling and timeouts (destination too slow to respond) transparent to the service client,
* handling identifiers and
* checking for data integrity across source and destination (Lightblue) entities.

The facade stands in front of the source and destination DAOs, directing traffic as needed. 4 different operations are supported:

### get

Read from source and destination (Lightblue) DAOs in parallel.

Compare returned entities. If differences are found, return source entity and log inconsistency. Otherwise, return destination entity.

Will call lightblue in dual read phase and beyond.

### update

Update entity using source and destination (Lightblue) DAOs in parallel. An update operation requires entity ID to be present.

Compare returned entities. If differences are found, return source entity and log inconsistency. Otherwise, return destination entity.

Will call lightblue in dual write phase and beyond.

### create

Create new entity using source and destination (Lightblue) DAOs in serial. First create in source to obtain ID. Once ID is assigned by source datastore, use it to create entity in destination. The destination (Lightblue) DAO needs to use EntityIdStore to get the ID and have a setter for DAOFacadeBase to set it.

Compare returned entities. If differences are found, return source entity and log inconsistency. Otherwise, return destination entity.

Will call lightblue in dual write phase and beyond.

### create with read

Same as create, except it will call lightblue in dual read phase and beyond.

## [How to configure togglz for migration to lightblue?](TOGGLZ.md)

## Notes

The facade was designed to work with DAO objects (data access layer), but it can be also used in front of a service bean (business logic layer). In case of the latter, you may need to duplicate business logic when preparing service bean implementation for lightblue. Logic duplication is generally undesired, but re-implementing business logic for lightblue may improve performance (you will have opportunities to merge multiple data operations, reducing the number of remote calls).

## Examples

See unit tests.
