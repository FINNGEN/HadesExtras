

rstudioapi::restartSession()

# run first using_CohortTableHandler.Rmd

cohortTableHandler

cds <- cohortTableHandler$cohortDefinitionSet
cds[1,"json"] <- cds[3,"json"]

CohortDiagnostics::executeDiagnostics(
  cohortDefinitionSet = cds,
  exportFolder = tempdir(),
  databaseId = "test",
  cohortDatabaseSchema = cohortTableHandler$cohortDatabaseSchema,
  databaseName = cohortTableHandler$databaseName,
  databaseDescription = "test",
  connection = cohortTableHandler$connectionHandler$getConnection(),
  cdmDatabaseSchema = cohortTableHandler$cdmDatabaseSchema,
  cohortTable = cohortTableHandler$cohortTableNames$cohortTable,
  vocabularyDatabaseSchema = cohortTableHandler$vocabularyDatabaseSchema,
  cohortIds = c(1,2),#  ,1778211,1778212),
  minCellCount = 1
  )



CohortDiagnostics::createMergedResultsFile(
  dataFolder = tempdir(),
  sqliteDbPath = file.path(tempdir(), "test.sqlite"),
  overwrite = TRUE

)

CohortDiagnostics::launchDiagnosticsExplorer(
  sqliteDbPath = file.path(tempdir(), "test.sqlite")
)








https://github.com/OHDSI/OhdsiShinyModules/blob/f3dc393bc90b3097b1b31506c3f47f5c3b953abf/R/cohort-diagnostics-main.R#L28C7-L28C27
