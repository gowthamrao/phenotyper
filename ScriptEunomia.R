# remotes::install_github("OHDSI/FeatureExtraction", ref = "cohortCovariates")
# remotes::install_github("OHDSI/SkeletonCohortDiagnostics")
options(error = traceback)
studyName <- "cohortSubsetEunomia"
originalCohortDefinitionSet <-
  CohortGenerator::getCohortDefinitionSet(
    settingsFileName = "settings/CohortsToCreate.csv",
    jsonFolder = "cohorts",
    sqlFolder = "sql/sql_server",
    packageName = "SkeletonCohortDiagnosticsStudy",
    cohortFileNameValue = "cohortId"
  ) %>%  dplyr::tibble() 

cohortDefinitionSet <- originalCohortDefinitionSet
connectionDetails <- Eunomia::getEunomiaConnectionDetails()

databaseId = 'eunomia'
databaseName = 'eunomia'
databaseDescription = 'eunomia'
cdmDatabaseSchema = 'main'
vocabularyDatabaseSchema = 'main'
cohortDatabaseSchema = 'main'
tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")


# Cohort Definitions ----
targetCohortIds <- c(17493)
  
subsetCohortIds <- c(17692, 17693)
featureCohortIds <- c(14907)

addCohortDefinitionSubset <-
  function(cohortDefinitionSet,
           subsetOperators,
           definitionId,
           targetCohortIds,
           overwriteExists = TRUE) {
    cohortSubsetDefintion = CohortGenerator::createCohortSubsetDefinition(name = '',
                                                                          definitionId = definitionId,
                                                                          subsetOperators = subsetOperators)
    
    modifiedCohortDefinitionSet <-
      CohortGenerator::addCohortSubsetDefinition(
        cohortDefinitionSet = cohortDefinitionSet,
        cohortSubsetDefintion = cohortSubsetDefintion,
        targetCohortIds = targetCohortIds,
        overwriteExisting = TRUE
      )
    
    return(modifiedCohortDefinitionSet)
  }

firstEvent <- CohortGenerator::createLimitSubset(
  name = "(First ever)",
  priorTime = 0,
  followUpTime = 0,
  limitTo = "firstEver"
)

earliestOccurrenceWith365 <- CohortGenerator::createLimitSubset(
  name = "(Earliest event with 365 days prior observation time)",
  priorTime = 365,
  followUpTime = 0,
  limitTo = "earliestRemaining"
)

lastEver <- CohortGenerator::createLimitSubset(
  name = "(Last ever event)",
  priorTime = 0,
  followUpTime = 0,
  limitTo = "lastEver"
)

maleOnly <-
  CohortGenerator::createDemographicSubset(name = "(Male only)",
                                           gender = 8532)

femaleOnly <-
  CohortGenerator::createDemographicSubset(name = "(Female only)",
                                           gender = 8507)

pediatricOnly <-
  CohortGenerator::createDemographicSubset(name = "(Pediatric only)",
                                           ageMax = 18)

adultOnly <-
  CohortGenerator::createDemographicSubset(name = "(Adult 18 to 65 only)",
                                           ageMin = 18,
                                           ageMax = 65)

adultMaleOnly <-
  CohortGenerator::createDemographicSubset(
    name = "(Adult 18 to 65 Male only)",
    gender = 8532,
    ageMin = 18,
    ageMax = 65
  )

adultFemaleOnly <-
  CohortGenerator::createDemographicSubset(
    name = "(Adult 18 to 65 Female only)",
    gender = 8507,
    ageMin = 18,
    ageMax = 65
  )

cohortDefinitionSet <-
  addCohortDefinitionSubset(
    cohortDefinitionSet = cohortDefinitionSet,
    subsetOperators = list(firstEvent),
    definitionId = 1,
    targetCohortIds = targetCohortIds
  )

cohortDefinitionSet <-
  addCohortDefinitionSubset(
    cohortDefinitionSet = cohortDefinitionSet,
    subsetOperators = list(earliestOccurrenceWith365),
    definitionId = 2,
    targetCohortIds = targetCohortIds
  )

cohortDefinitionSet <-
  addCohortDefinitionSubset(
    cohortDefinitionSet = cohortDefinitionSet,
    subsetOperators = list(lastEver),
    definitionId = 3,
    targetCohortIds = targetCohortIds
  )

cohortDefinitionSet <-
  addCohortDefinitionSubset(
    cohortDefinitionSet = cohortDefinitionSet,
    subsetOperators = list(maleOnly),
    definitionId = 4,
    targetCohortIds = targetCohortIds
  )

cohortDefinitionSet <-
  addCohortDefinitionSubset(
    cohortDefinitionSet = cohortDefinitionSet,
    subsetOperators = list(femaleOnly),
    definitionId = 5,
    targetCohortIds = targetCohortIds
  )

cohortDefinitionSet <-
  addCohortDefinitionSubset(
    cohortDefinitionSet = cohortDefinitionSet,
    subsetOperators = list(pediatricOnly),
    definitionId = 6,
    targetCohortIds = targetCohortIds
  )

cohortDefinitionSet <-
  addCohortDefinitionSubset(
    cohortDefinitionSet = cohortDefinitionSet,
    subsetOperators = list(adultOnly),
    definitionId = 7,
    targetCohortIds = targetCohortIds
  )

cohortDefinitionSet <-
  addCohortDefinitionSubset(
    cohortDefinitionSet = cohortDefinitionSet,
    subsetOperators = list(adultMaleOnly),
    definitionId = 8,
    targetCohortIds = targetCohortIds
  )

cohortDefinitionSet <-
  addCohortDefinitionSubset(
    cohortDefinitionSet = cohortDefinitionSet,
    subsetOperators = list(adultFemaleOnly),
    definitionId = 9,
    targetCohortIds = targetCohortIds
  )

# remove persons in second from 1st - this gives

cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable =
                                                          paste0("c", studyName))

# output folder information ----
outputFolder <-
  file.path("D:", "temp", "outputFolder", studyName, databaseId)
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
cohortGenerated <- CohortGenerator::generateCohortSet(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  tempEmulationSchema = tempEmulationSchema,
  cohortTableNames = cohortTableNames,
  cohortDefinitionSet = cohortDefinitionSet,
  cohortDatabaseSchema = cohortDatabaseSchema,
  incremental = TRUE,
  incrementalFolder = file.path(outputFolder, "incremental")
)


FeatureExtractionSettingsCohortDiagnostics <-
  CohortDiagnostics::getDefaultCovariateSettings()

FeatureExtractionSettingsCohortBasedCovariateSettings <-
  FeatureExtraction::createCohortBasedTemporalCovariateSettings(
    analysisId = 150,
    covariateCohortDatabaseSchema = cohortDatabaseSchema,
    covariateCohortTable = cohortTableNames$cohortTable,
    covariateCohorts = cohortDefinitionSet |> 
      dplyr::filter(cohortId %in% c(featureCohortIds)) |>
      dplyr::select(cohortId,
                    cohortName),
    valueType = "binary",
    temporalStartDays = FeatureExtractionSettingsCohortDiagnostics$temporalStartDays,
    temporalEndDays = FeatureExtractionSettingsCohortDiagnostics$temporalEndDays
  )
FeatureExtractionCovariateSettings <- 
  list(
    FeatureExtractionSettingsCohortBasedCovariateSettings,
    FeatureExtractionSettingsCohortDiagnostics
  )

connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)


# just simple plain vanile covariate settings
featureExtractionOutput1 <-
  FeatureExtraction::getDbCovariateData(
    connection = connection,
    oracleTempSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableNames$cohortTable,
    covariateSettings = FeatureExtraction::createDefaultTemporalCovariateSettings(),
    aggregated = TRUE
  )

# cohort as covariate setting
featureExtractionOutput2 <-
  FeatureExtraction::getDbCovariateData(
    connection = connection,
    oracleTempSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableNames$cohortTable,
    covariateSettings = FeatureExtraction::createDefaultTemporalCovariateSettings(),
    aggregated = TRUE
  )

# using a more complicated list of covariate settings
featureExtractionOutput3 <-
  FeatureExtraction::getDbCovariateData(
    connection = connection,
    oracleTempSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableNames$cohortTable,
    covariateSettings = FeatureExtractionCovariateSettings,
    aggregated = TRUE
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
  runIncidenceRate = FALSE,
  runCohortRelationship = FALSE,
  vocabularyDatabaseSchema = vocabularyDatabaseSchema,
  temporalCovariateSettings = FeatureExtractionSettingsCohortBasedCovariateSettings, ## change to FeatureExtractionCovariateSettings if list
  incremental = TRUE
)


# package results ----
wd <- getwd()
setwd(outputFolder)
CohortDiagnostics::createMergedResultsFile(dataFolder = outputFolder, overwrite = TRUE)
# Launch diagnostics explorer shiny app ----
CohortDiagnostics::launchDiagnosticsExplorer()


######

diagnosticsFileName <- "CreatedDiagnostics.csv"

listFiles <-
  list.files(
    path = outputFolder,
    pattern = diagnosticsFileName,
    full.names = TRUE,
    recursive = TRUE
  )

# "getCohortCounts", "runInclusionStatistics", "runIncludedSourceConcepts",
# "runBreakdownIndexEvents", "runOrphanConcepts", 
# "runVisitContext", "runIncidenceRate", "runCohortOverlap","runCohortAsFeatures",
# "runTemporalCohortCharacterization"


tasksToRemove <- c("runTemporalCohortCharacterization")


for (i in (1:length(listFiles))) {
  readr::read_csv(
    file = listFiles[[i]],
    col_types = readr::cols(),
    guess_max = min(1e7)
  ) %>%
    dplyr::filter(!task %in% tasksToRemove) %>%
    readr::write_excel_csv(file = listFiles[[i]])
}
