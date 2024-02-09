# writeThe roxigen header
#' @title CohortDiagnostics_mergeCsvResults
#' @description
#' Merge the results of multiple CohortDiagnostics into a single folder
#' @param pathToResultFolders character vector of paths to the result folders
#' @param pathToMergedRestulsFolder path to the folder where the merged results will be stored
#' @return path to the merged results folder
#'
#' @importFrom checkmate assertCharacter assertDirectoryExists
#' @importFrom base file.copy
#' @importFrom brio writeLines
#'
#' @export
CohortDiagnostics_mergeCsvResults <- function(pathToResultFolders, pathToMergedRestulsFolder) {

  checkmate::assertCharacter(pathToResultFolders)
  for (pathToResultFolder in pathToResultFolders) {
    checkmate::assertDirectoryExists(pathToResultFolder)
  }

  if (length(pathToResultFolders) == 1) {
    return(pathToResultFolders)
  }

  fileNames <- list.files(pathToResultFolders[1], pattern = "csv$")
  for (fileName in fileNames) {
    file.copy(from = file.path(pathToResultFolders[1], fileName), to = pathToMergedRestulsFolder)
  }

  for (pathToResultFolder in pathToResultFolders[2:length(pathToResultFolders)]) {
    for (fileName in fileNames) {
      file_content <- readLines(file.path(pathToResultFolder, fileName))[-1]
      cat(file_content, file = file.path(pathToMergedRestulsFolder, fileName),sep = "\n",append = TRUE)
    }


  }

  return(pathToMergedRestulsFolder)
}

