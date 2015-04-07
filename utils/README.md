# Lightblue-migrator-utils

Lightblue-migrator-utils provides some aid with migration to Lightblue. The key class in this module is DAOFacadeBase, which offers:
* phased migration using feature flag support (Togglz - see LightblueMigrationFeatures),
* parallel processing (when possible),
* handling identifiers and
* checking for data integrity across source and destination (Lightblue) entities.

DAOFacadeBase stands in front of the source and destination DAOs, directing traffic as needed. 3 different operations are supported:

### get

Read from source and destination (Lightblue) DAOs in parallel.

Compare returned entities. If differences are found, return source entity and log inconsistency. Otherwise, return destination entity.

### update

Update entity using source and destination (Lightblue) DAOs in parallel. An update operation requires entity ID to be present.

Compare returned entities. If differences are found, return source entity and log inconsistency. Otherwise, return destination entity.

### create

Create new entity using source and destination (Lightblue) DAOs in serial. First create in source to obtain ID. Once ID is assigned by source datastore, use it to create entity in destination. The destination (Lightblue) DAO needs to use EntityIdStore to get the ID and have a setter for DAOFacadeBase to set it.

Compare returned entities. If differences are found, return source entity and log inconsistency. Otherwise, return destination entity.

## Notes

* Ensure your DAO layer is as simple as possible by moving business logic to upper layers.
* If your DAO operation does more than one kind of operations defined above (for example, gets, then writes), treat it as a more affecting operation (get->update->create).

## Examples

See unit tests.