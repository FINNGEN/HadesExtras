# CohortTableHandler

Class for handling cohort tables in a CDM database. Inherits from
CDMdbHandler.

## Super class

[`HadesExtras::CDMdbHandler`](https://finngen.github.io/HadesExtras/reference/CDMdbHandler.md)
-\> `CohortTableHandler`

## Active bindings

- `cohortDatabaseSchema`:

  Schema where cohort tables are stored

- `cohortTableNames`:

  Names of the cohort tables in the database

- `incrementalFolder`:

  Path to folder for incremental operations

- `cohortDefinitionSet`:

  Set of cohort definitions

- `cohortGeneratorResults`:

  Results from cohort generation process

- `cohortDemograpics`:

  Demographic information for cohorts

- `cohortsOverlap`:

  Information about overlapping cohorts

## Methods

### Public methods

- [`CohortTableHandler$new()`](#method-CohortTableHandler-new)

- [`CohortTableHandler$loadConnection()`](#method-CohortTableHandler-loadConnection)

- [`CohortTableHandler$insertOrUpdateCohorts()`](#method-CohortTableHandler-insertOrUpdateCohorts)

- [`CohortTableHandler$deleteCohorts()`](#method-CohortTableHandler-deleteCohorts)

- [`CohortTableHandler$getCohortCounts()`](#method-CohortTableHandler-getCohortCounts)

- [`CohortTableHandler$getNumberOfSubjects()`](#method-CohortTableHandler-getNumberOfSubjects)

- [`CohortTableHandler$getNumberOfCohortEntries()`](#method-CohortTableHandler-getNumberOfCohortEntries)

- [`CohortTableHandler$getCohortsSummary()`](#method-CohortTableHandler-getCohortsSummary)

- [`CohortTableHandler$getCohortIdAndNames()`](#method-CohortTableHandler-getCohortIdAndNames)

- [`CohortTableHandler$updateCohortNames()`](#method-CohortTableHandler-updateCohortNames)

- [`CohortTableHandler$getCohortsOverlap()`](#method-CohortTableHandler-getCohortsOverlap)

- [`CohortTableHandler$getNumberOfOverlappingSubjects()`](#method-CohortTableHandler-getNumberOfOverlappingSubjects)

- [`CohortTableHandler$getSexFisherTest()`](#method-CohortTableHandler-getSexFisherTest)

- [`CohortTableHandler$getYearOfBirthTests()`](#method-CohortTableHandler-getYearOfBirthTests)

- [`CohortTableHandler$clone()`](#method-CohortTableHandler-clone)

------------------------------------------------------------------------

### Method `new()`

Initialize the CohortTableHandler object

#### Usage

    CohortTableHandler$new(
      connectionHandler,
      databaseId,
      databaseName,
      databaseDescription,
      cdmDatabaseSchema,
      vocabularyDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema,
      cohortTableName,
      loadConnectionChecksLevel = "allChecks"
    )

#### Arguments

- `connectionHandler`:

  The connection handler object.

- `databaseId`:

  A text id for the database the it connects to

- `databaseName`:

  A text name for the database the it connects to

- `databaseDescription`:

  A text description for the database the it connects to

- `cdmDatabaseSchema`:

  Name of the CDM database schema.

- `vocabularyDatabaseSchema`:

  Name of the vocabulary database schema. Default is the same as the CDM
  database schema.

- `cohortDatabaseSchema`:

  Name of the cohort database schema.

- `cohortTableName`:

  Name of the cohort table.

- `loadConnectionChecksLevel`:

  (Optional) Level of checks to perform when loading the connection
  (default is "allChecks")

  loadConnection

------------------------------------------------------------------------

### Method `loadConnection()`

Reloads the connection with the initial setting and updates connection
status

insertOrUpdateCohorts

#### Usage

    CohortTableHandler$loadConnection(loadConnectionChecksLevel)

#### Arguments

- `loadConnectionChecksLevel`:

  Level of checks to perform during connection

------------------------------------------------------------------------

### Method `insertOrUpdateCohorts()`

If there is no cohort with the same cohortId it is added to the
cohortDefinitionSet, If there is a cohort with the same cohortId, the
cohort is updated in the cohortDefinitionSet CohortDefinitionSet is
generated and demographics is updated for only the cohorts that have
changed

#### Usage

    CohortTableHandler$insertOrUpdateCohorts(cohortDefinitionSet)

#### Arguments

- `cohortDefinitionSet`:

  The cohort definition set to add.

  deleteCohorts

------------------------------------------------------------------------

### Method `deleteCohorts()`

Deletes cohorts from the cohort table.

#### Usage

    CohortTableHandler$deleteCohorts(cohortIds)

#### Arguments

- `cohortIds`:

  The cohort ids to delete.

  getCohortCounts

------------------------------------------------------------------------

### Method `getCohortCounts()`

Retrieves cohort counts from the cohort table.

#### Usage

    CohortTableHandler$getCohortCounts()

#### Returns

A tibble containing the cohort counts with names.

getNumberOfSubjects

------------------------------------------------------------------------

### Method `getNumberOfSubjects()`

Retrieves the number of subjects of a cohort ID.

#### Usage

    CohortTableHandler$getNumberOfSubjects(selected_cohortId)

#### Arguments

- `selected_cohortId`:

  The cohort id for which number of subjects is sought.

#### Returns

A numeric value of the number of subjects in a cohort.

getNumberOfCohortEntries

------------------------------------------------------------------------

### Method `getNumberOfCohortEntries()`

Retrieves the number of entries of a cohort ID.

#### Usage

    CohortTableHandler$getNumberOfCohortEntries(selected_cohortId)

#### Arguments

- `selected_cohortId`:

  The cohort id for which number of entries is sought.

#### Returns

A numeric value of the number of entries in a cohort.

getCohortsSummary

------------------------------------------------------------------------

### Method `getCohortsSummary()`

Retrieves the summary of cohorts including cohort start and end year
histograms and sex counts.

#### Usage

    CohortTableHandler$getCohortsSummary(includeAllEvents = F)

#### Arguments

- `includeAllEvents`:

  Logical, whether to include all events or just subjects. Default is
  FALSE.

#### Returns

A tibble containing cohort summary.

getCohortIdAndNames

------------------------------------------------------------------------

### Method `getCohortIdAndNames()`

Retrieves the cohort names.

#### Usage

    CohortTableHandler$getCohortIdAndNames()

#### Returns

A vector with the name of the cohorts

updateCohortNames

------------------------------------------------------------------------

### Method `updateCohortNames()`

Updates the cohort name and short name.

#### Usage

    CohortTableHandler$updateCohortNames(cohortId, newCohortName, newShortName)

#### Arguments

- `cohortId`:

  The cohort id to update.

- `newCohortName`:

  New name to assign to the cohort

- `newShortName`:

  New short name to assign to the cohort

- `cohortName`:

  The new cohort name.

- `shortName`:

  The new short name.

  getCohortsOverlap

------------------------------------------------------------------------

### Method `getCohortsOverlap()`

Retrieves the number of subjects that are in more than one cohort.

#### Usage

    CohortTableHandler$getCohortsOverlap()

#### Returns

A tibble containing one logical column for each cohort with name a
cohort id, and an additional column `numberOfSubjects` with the number
of subjects in the cohorts combination.

getNumberOfOverlappingSubjects

------------------------------------------------------------------------

### Method `getNumberOfOverlappingSubjects()`

Retrieves the number of subjects that are overlapping between two given
cohorts.

#### Usage

    CohortTableHandler$getNumberOfOverlappingSubjects(
      selected_cohortId1,
      selected_cohortId2
    )

#### Arguments

- `selected_cohortId1`:

  The cohort id of the first cohort.

- `selected_cohortId2`:

  The cohort id of the second cohort.

#### Returns

A numeric value of the number of overlapping subjects between two given
cohorts.

getSexFisherTest

------------------------------------------------------------------------

### Method `getSexFisherTest()`

Compares the proportion of males and females in two cohorts using
Fisher's exact test.

#### Usage

    CohortTableHandler$getSexFisherTest(
      selected_cohortId1,
      selected_cohortId2,
      testFor = "Subjects"
    )

#### Arguments

- `selected_cohortId1`:

  The cohort id of the first cohort.

- `selected_cohortId2`:

  The cohort id of the second cohort.

- `testFor`:

  Character string indicating what to test: "Subjects" or "allEvents".
  Default is "Subjects".

#### Returns

a list containing components such as p.value and conf.int of the test

getYearOfBirthTests

------------------------------------------------------------------------

### Method `getYearOfBirthTests()`

Compares the year of birth distributions between the case and control
cohorts using two-sample t-test to compare mean year of birth, uses
cohen's d to assess effect size of the difference in the mean of the
year of births,and the Kolmogorov-Smirnov test to evaluate if year of
births in the two cohorts have similar distribution.

#### Usage

    CohortTableHandler$getYearOfBirthTests(
      selected_cohortId1,
      selected_cohortId2,
      testFor = "Subjects"
    )

#### Arguments

- `selected_cohortId1`:

  The cohort id of the first cohort.

- `selected_cohortId2`:

  The cohort id of the second cohort.

- `testFor`:

  The type of test to perform. "Subjects" to test the year of birth of
  the subjects in the cohorts, "allEvents" to test the year of birth of
  all events in the cohorts.

#### Returns

a list with with three members ttestResult (R htest object),
kstestResult (R htest object), cohend result (list of meanInCases,
meanInControls, pooledsd, and cohend)

------------------------------------------------------------------------

### Method `clone()`

The objects of this class are cloneable with this method.

#### Usage

    CohortTableHandler$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.
