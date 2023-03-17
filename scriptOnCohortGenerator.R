# remotes::install_github("OHDSI/FeatureExtraction")#, ref = "cohortCovariates")

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
  dplyr::filter(cohortId == 259) |>
  dplyr::pull(cohortId) |>
  unique()
subsetCohortIds <- PhenotypeLibrary::getPhenotypeLog() |>
  dplyr::filter(stringr::str_detect(string = hashTag, pattern = "#Visits")) |>
  dplyr::filter(cohortId %in% c(23, 24)) |>
  dplyr::pull(cohortId) |>
  unique()
featureCohortIds <- PhenotypeLibrary::getPhenotypeLog() |>
  dplyr::filter(stringr::str_detect(string = hashTag, pattern = "#Symptoms")) |>
  dplyr::pull(cohortId) |>
  unique()

cohortDefinitionSet <-
  PhenotypeLibrary::getPlCohortDefinitionSet(cohortIds = c(targetCohortIds) |> unique())

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


FeatureExtractionSettingsCohortDiagnostics <-
  CohortDiagnostics::getDefaultCovariateSettings()
FeatureExtractionSettingsCohortBasedCovariateSettings <-
  FeatureExtraction::createCohortBasedTemporalCovariateSettings(
    analysisId = 150,
    covariateCohortDatabaseSchema = cohortDatabaseSchema,
    covariateCohortTable = paste0(
      stringr::str_squish("pl_"),
      stringr::str_squish(cdmSource$sourceKey)
    ),
    covariateCohorts =
      PhenotypeLibrary::getPlCohortDefinitionSet(cohortIds = featureCohortIds |> unique()),
    valueType = "binary",
    temporalStartDays = FeatureExtractionSettingsCohortDiagnostics$temporalStartDays,
    temporalEndDays = FeatureExtractionSettingsCohortDiagnostics$temporalEndDays
  )
FeatureExtractionCovariateSettings <-
  list(
    FeatureExtractionSettingsCohortDiagnostics,
    FeatureExtractionSettingsCohortBasedCovariateSettings
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
  temporalCovariateSettings = FeatureExtractionSettingsCohortDiagnostics,
  #FeatureExtractionCovariateSettings,
  incremental = TRUE
)


# package results ----
wd <- getwd()
setwd(outputFolder)
CohortDiagnostics::createMergedResultsFile(dataFolder = outputFolder, overwrite = TRUE)
# Launch diagnostics explorer shiny app ----
CohortDiagnostics::launchDiagnosticsExplorer()
