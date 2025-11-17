# CDM Database Handler Class

A class for handling CDM database connections and operations

## Methods

- `initialize(databaseId)`:

  Initialize a new CDM database handler

- `loadConnection(loadConnectionChecksLevel)`:

  Load database connection with specified check level

## Active bindings

- `databaseId`:

  The ID of the database

- `databaseName`:

  The name of the database

- `databaseDescription`:

  Description of the database

- `connectionHandler`:

  Handler for database connections

- `vocabularyDatabaseSchema`:

  Schema name for the vocabulary database

- `cdmDatabaseSchema`:

  Schema name for the CDM database

- `resultsDatabaseSchema`:

  Schema name for the results database

- `connectionStatusLog`:

  Log of connection status and operations

- `vocabularyInfo`:

  Information about the vocabulary tables

- `CDMInfo`:

  Information about the CDM structure

- `getTblVocabularySchema`:

  Function to get vocabulary schema table

- `getTblCDMSchema`:

  Function to get CDM schema table

## Methods

### Public methods

- [`CDMdbHandler$new()`](#method-CDMdbHandler-new)

- [`CDMdbHandler$loadConnection()`](#method-CDMdbHandler-loadConnection)

- [`CDMdbHandler$clone()`](#method-CDMdbHandler-clone)

------------------------------------------------------------------------

### Method `new()`

Returns the results database schema name.

#### Usage

    CDMdbHandler$new(
      databaseId,
      databaseName,
      databaseDescription,
      connectionHandler,
      cdmDatabaseSchema,
      vocabularyDatabaseSchema = cdmDatabaseSchema,
      resultsDatabaseSchema = cdmDatabaseSchema,
      loadConnectionChecksLevel = "allChecks"
    )

#### Arguments

- `databaseId`:

  A text id for the database the it connects to

- `databaseName`:

  A text name for the database the it connects to

- `databaseDescription`:

  A text description for the database the it connects to

- `connectionHandler`:

  A ConnectionHandler object

- `cdmDatabaseSchema`:

  Name of the CDM database schema

- `vocabularyDatabaseSchema`:

  (Optional) Name of the vocabulary database schema (default is
  cdmDatabaseSchema)

- `resultsDatabaseSchema`:

  (Optional) Name of the results database schema (default is
  cdmDatabaseSchema)

- `loadConnectionChecksLevel`:

  (Optional) Level of checks to perform when loading the connection
  (default is "allChecks") Reload connection

------------------------------------------------------------------------

### Method `loadConnection()`

Updates the connection status by checking the database connection,
vocabulary database schema, and CDM database schema.

#### Usage

    CDMdbHandler$loadConnection(loadConnectionChecksLevel)

#### Arguments

- `loadConnectionChecksLevel`:

  Level of connection checks to perform

------------------------------------------------------------------------

### Method `clone()`

The objects of this class are cloneable with this method.

#### Usage

    CDMdbHandler$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.
