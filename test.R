remotes::install_github("OHDSI/Strategus")
remotes::install_github("OHDSI/CohortGenerator")

#Create Strategus Input Specifications ----
cohortTableName = "cohort"
outputFolder <- file.path("D:", "test", "strategusCG")
# unlink(x = outputFolder, recursive = TRUE, force = TRUE)
dir.create(path = outputFolder,
           showWarnings = FALSE,
           recursive = TRUE)

cohortDefinitionSet <-
  CohortGenerator::getCohortDefinitionSet(
    settingsFileName = "settings/CohortsToCreate.csv",
    jsonFolder = "cohorts",
    sqlFolder = "sql/sql_server",
    packageName = "SkeletonCohortDiagnosticsStudy",
    cohortFileNameValue = "cohortId"
  ) |>  dplyr::tibble()

targetCohortDefinitionSet <-
  cohortDefinitionSet |>
  dplyr::filter(stringr::str_detect(string = tolower(cohortName),
                                    pattern = "diclofenac"))
outcomeCohortDefinitionSet <-
  cohortDefinitionSet |>
  dplyr::filter(stringr::str_detect(string = tolower(cohortName),
                                    pattern = "hemorrhage"))


# copy over pregenerated cohorts

cohortDefinitionSet <- dplyr::bind_rows(targetCohortDefinitionSet,
                                        outcomeCohortDefinitionSet) |>
  dplyr::arrange(cohortId) |>
  dplyr::distinct()
## -- add subset operations using cohortGenerator functionality.


#Step 1: Cohort Generator ----
source(
  "https://raw.githubusercontent.com/OHDSI/CohortGeneratorModule/v0.1.0/SettingsFunctions.R"
)
cohortDefinitionShared <-
  createCohortSharedResourceSpecifications(cohortDefinitionSet = cohortDefinitionSet)

cohortGeneratorModuleSpecifications <-
  createCohortGeneratorModuleSpecifications(incremental = TRUE,
                                            generateStats = TRUE) 


#Step 2: Characterization ----
source(
  "https://raw.githubusercontent.com/OHDSI/CharacterizationModule/v0.2.3/SettingsFunctions.R" # latest on 2/18/2023
)

characterizationModuleSpecifications <-
  createCharacterizationModuleSpecifications(
    targetIds = targetCohortDefinitionSet$cohortId,
    outcomeIds = outcomeCohortDefinitionSet$cohortId,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 30,
    timeAtRisk = data.frame(
      riskWindowStart = c(1, 1),
      startAnchor = c("cohort start", "cohort start"),
      riskWindowEnd = c(365, 99999),
      endAnchor = c("cohort start", "cohort start")
    ),
    covariateSettings = FeatureExtraction::createDefaultCovariateSettings()
  )


#Step 3: Chain Strategus input specification ----
analysisSpecifications <-
  Strategus::createEmptyAnalysisSpecificiations() |>
  Strategus::addSharedResources(cohortDefinitionShared) |>
  Strategus::addModuleSpecifications(cohortGeneratorModuleSpecifications) |>
  Strategus::addModuleSpecifications(characterizationModuleSpecifications)

ParallelLogger::saveSettingsToJson(analysisSpecifications,
                                   file.path(outputFolder, 'settings.json'))

analysisSpecifications <-
  ParallelLogger::loadSettingsFromJson(file.path(outputFolder, 'settings.json'))


# Step 4: Execute strategus ----
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
connectionDetailsReference <- "eunomia"
workDatabaseSchema <- "main"
cdmDatabaseSchema <- "main"
cohortTableNames <-
  CohortGenerator::getCohortTableNames(cohortTable = cohortTableName)
outputLocation <- file.path(outputFolder, "results")
resultsFolder <- file.path(outputLocation, "strategusOutput")
workFolder <- file.path(outputLocation, "strategusWork")
minCellCount <- 0

Strategus::storeConnectionDetails(connectionDetails = connectionDetails,
                                  connectionDetailsReference = connectionDetailsReference)

executionSettings <- Strategus::createCdmExecutionSettings(
  connectionDetailsReference = connectionDetailsReference,
  workDatabaseSchema = workDatabaseSchema,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortTableNames = cohortTableNames,
  workFolder = workFolder,
  resultsFolder = resultsFolder,
  minCellCount = minCellCount
)


# Note: this environmental variable should be set once for each compute node
Sys.setenv("INSTANTIATED_MODULES_FOLDER" = file.path(outputLocation, "StrategusInstantiatedModules"))

Strategus::execute(
  analysisSpecifications = analysisSpecifications,
  executionSettings = executionSettings,
  restart = FALSE,
  executionScriptFolder = file.path(outputLocation, "strategusExecution")
)


# Step 5: upload results to remote db ----
sourcesToUpload <-
  list.dirs(resultsFolder, recursive = FALSE, full.names = FALSE)

outputSqlLiteDb <- file.path(outputLocation,
                             "remote.sqlite")
if (file.exists(outputSqlLiteDb)) {
  unlink(x = outputSqlLiteDb,
         recursive = TRUE,
         force = TRUE)
}
connectionDetails <-
  DatabaseConnector::createConnectionDetails(dbms = "sqlite",
                                             server = outputSqlLiteDb)
connection <-
  DatabaseConnector::connect(connectionDetails = connectionDetails)
resultsDatabaseSchema <- "main"

createResultsTable <- FALSE
resultsDatabaseTableNames <-
  DatabaseConnector::getTableNames(connection = connection,
                                   databaseSchema = resultsDatabaseSchema)

if (length(resultsDatabaseTableNames) == 0) {
  createResultsTable <- TRUE
}

for (i in 1:length(sourcesToUpload)) {
  isModuleComplete <- function(modulePath) {
    # To identify a module that has finished, find the "done" file
    # in the folder which signals the module ran without issue
    doneFileFound <-
      (length(list.files(path = modulePath, pattern = "done")) > 0)
    isDatabaseMetaDataFolder <-
      basename(modulePath) == "DatabaseMetaData"
    return(doneFileFound || isDatabaseMetaDataFolder)
  }
  
  sourceKey <- sourcesToUpload[i]
  resultsPath <- file.path(resultsFolder, sourceKey)
  rlang::inform(paste0("Loading results for: ", sourceKey, " in ", resultsPath))
  
  moduleIsComplete <- isModuleComplete(resultsPath)
  
  if (!moduleIsComplete) {
    rlang::inform(paste0("Module incomplete: ", sourceKey, " in ", resultsPath))
  }
  
  resultsDataModelSpecifications <-
    readr::read_csv(
      file = file.path(resultsPath,
                       "resultsDataModelSpecification.csv"),
      col_types = readr::cols()
    )
  expectedResultTables <-
    resultsDataModelSpecifications$table_name |> unique()
  resultFiles <-
    list.files(path = resultsPath,
               recursive = FALSE,
               pattern = ".csv") |>
    gsub(pattern = ".csv", replacement = "")
  observedResultFiles <-
    intersect(x = expectedResultTables, y = resultFiles)
  
  
  # Create the destination tables if this is the first source we are uploading
  rlang::inform("Creating tables")
  sql <-
    ResultModelManager::generateSqlSchema(
      schemaDefinition = resultsDataModelSpecifications |> SqlRender::snakeCaseToCamelCaseNames(),
      overwrite = TRUE
    )
  
  DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                               sql = sql,
                                               database_schema = resultsDatabaseSchema)
  # https://github.com/OHDSI/ResultModelManager/issues/21
  # ResultModelManager::createResultsDataModel(connection = connection,
  #                                            schema = resultsDatabaseSchema,
  #                                            sql = sql)
  
  ResultModelManager::uploadResults(
    connection = connection,
    schema = resultsDatabaseSchema,
    resultsFolder = resultsPath,
    forceOverWriteOfSpecifications = TRUE,
    purgeSiteDataBeforeUploading = TRUE,
    databaseIdentifierFile = file.path(
      resultsFolder,
      "DatabaseMetaData",
      "database_meta_data.csv"
    ),
    tablePrefix = "",
    runCheckAndFixCommands = FALSE,
    specifications = resultsDataModelSpecifications |> SqlRender::snakeCaseToCamelCaseNames()
  )
}
DatabaseConnector::disconnect(connection = connection)

#Step 5: Create Shiny ----
config <- ShinyAppBuilder::initializeModuleConfig() |>
  ShinyAppBuilder::addModuleConfig(
    ShinyAppBuilder::createDefaultAboutConfig(resultDatabaseDetails = list(),
                                              useKeyring = F)
  ) |>
  ShinyAppBuilder::addModuleConfig(ShinyAppBuilder::createDefaultCohortGeneratorConfig()) |>
  ShinyAppBuilder::addModuleConfig(ShinyAppBuilder::createDefaultCharacterizationConfig()) 


# create a connection handler using the ResultModelManager package
connection <-
  ResultModelManager::ConnectionHandler$new(connectionDetails)

# now create the shiny app based on the config file and view the results
# based on the connection
ShinyAppBuilder::createShinyApp(config = config, connection = connection)
##ShinyAppBuilder::viewShiny(config = config, connection = connection)

DatabaseConnector::disconnect(connection = connection)
