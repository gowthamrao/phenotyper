remotes::install_github("OHDSI/FeatureExtraction", ref = "cohortCovariates")

studyName <- "cohortSubset"

# connection details ----
connectionSpecifications <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'truven_mdcd')

cdmSource <- getCdmSource()

connectionDetails <-
  DatabaseConnector::createConnectionDetails(
    dbms = cdmSource$dbms,
    user = keyring::key_get(service = userNameService),
    password = keyring::key_get(service = passwordService),
    port = cdmSource$port,
    server = cdmSource$serverFinal
  )

databaseId = cdmSource$sourceKey
databaseName = cdmSource$sourceName
databaseDescription = cdmSource$sourceName
cdmDatabaseSchema = cdmSource$cdmDatabaseSchemaFinal
vocabularyDatabaseSchema = cdmSource$vocabDatabaseSchemaFinal
cohortDatabaseSchema = cdmSource$cohortDatabaseSchemaFinal
tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")


# Cohort Definitions ----
targetCohortIds <- PhenotypeLibrary::getPhenotypeLog() |> 
  dplyr::filter(stringr::str_detect(string = tolower(cohortName), pattern = "anaphylaxis")) |> 
  dplyr::pull(cohortId) |> 
  unique()
subsetCohortIds <- PhenotypeLibrary::getPhenotypeLog() |> 
  dplyr::filter(stringr::str_detect(string = hashTag, pattern = "#Visits")) |> 
  dplyr::pull(cohortId) |> 
  unique()
featureCohortIds <- PhenotypeLibrary::getPhenotypeLog() |> 
  dplyr::filter(stringr::str_detect(string = hashTag, pattern = "#Visits")) |> 
  dplyr::pull(cohortId) |> 
  unique()

cohortDefinitionSet <-
  PhenotypeLibrary::getPlCohortDefinitionSet(cohortIds = c(targetCohortIds, subsetCohortIds, featureCohortIds) |> unique())

cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable =
                                                          paste0("c", studyName, "_", cdmSource$sourceId))

# output folder information ----
outputFolder <-
  file.path("D:", "temp", "outputFolder", studyName, cdmSource$sourceKey)
## optionally delete previous execution ----
# unlink(x = outputFolder,
#        recursive = TRUE,
#        force = TRUE)
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

cohortDiagnosticsDefaultTemporalCovariateSettings <-
  CohortDiagnostics::createDefaultTemporalCovaraiteSettings()

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
