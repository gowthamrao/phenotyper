# connection details ----
remotes::install_github('OHDSI/Eunomia')
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
databaseId = "Eunomia"
databaseName = "Eunomia Test"
databaseDescription = "This is a test data base called Eunomia"
cdmDatabaseSchema = 'main'
vocabularyDatabaseSchema = "main"
cohortDatabaseSchema = "main"
tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")

# Cohort Definitions ----
remotes::install_github('OHDSI/SkeletonCohortDiagnosticsStudy')

## get cohort definition set ----
cohortDefinitionSet <-
  CohortGenerator::getCohortDefinitionSet(
    settingsFileName = "settings/CohortsToCreate.csv",
    jsonFolder = "cohorts",
    sqlFolder = "sql/sql_server",
    packageName = "SkeletonCohortDiagnosticsStudy",
    cohortFileNameValue = "cohortId"
  ) |>  dplyr::tibble()

subsetEarliestRemaining <- CohortGenerator::createLimitSubset(
  name = "Observation of at least 365 days prior",
  priorTime = 365,
  followUpTime = 0,
  limitTo = "earliestRemaining"
)

subsetDef <- CohortGenerator::createCohortSubsetDefinition(
  name = "Patients in target cohort with 365 days prior observation",
  definitionId = 1,
  subsetOperators = list(subsetEarliestRemaining)
)

cohortDefinitionSet <- cohortDefinitionSet |> 
  CohortGenerator::addCohortSubsetDefinition(subsetDef)

cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = "cohortEunomia")

# output folder information ----
outputFolder <-
  file.path("D:", "temp", "outputFolder", "eunomia")

## optionally delete previous execution ----
unlink(x = outputFolder,
       recursive = TRUE,
       force = TRUE)
dir.create(path = outputFolder,
           showWarnings = FALSE,
           recursive = TRUE)

# Execution ----
## Create cohort tables on remote ----
CohortGenerator::createCohortTables(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTableNames = cohortTableNames,
  incremental = TRUE
)
## Generate cohort on remote ----
CohortGenerator::generateCohortSet(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  tempEmulationSchema = tempEmulationSchema,
  cohortTableNames = cohortTableNames,
  cohortDefinitionSet = cohortDefinitionSet,
  cohortDatabaseSchema = cohortDatabaseSchema,
  incremental = TRUE,
  incrementalFolder = file.path(outputFolder, "incremental")
)

## Execute Cohort Diagnostics on remote ----
CohortDiagnostics::executeDiagnostics(
  cohortDefinitionSet = cohortDefinitionSet,
  exportFolder = outputFolder,
  databaseId = databaseId,
  databaseName = databaseName,
  databaseDescription = databaseDescription,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cdmDatabaseSchema = cdmDatabaseSchema,
  tempEmulationSchema = tempEmulationSchema,
  connectionDetails = connectionDetails,
  cohortTableNames = cohortTableNames,
  vocabularyDatabaseSchema = vocabularyDatabaseSchema,
  incremental = TRUE
)


# package results ----
CohortDiagnostics::createMergedResultsFile(dataFolder = outputFolder, overwrite = TRUE)
# Launch diagnostics explorer shiny app ----
CohortDiagnostics::launchDiagnosticsExplorer()
