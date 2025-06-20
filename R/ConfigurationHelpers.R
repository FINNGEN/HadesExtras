#' Read and Parse YAML File with Placeholder Replacement
#'
#' Reads a YAML file from a given path, replaces specified placeholders with provided values,
#' and returns the parsed content. If any provided placeholders are not found in the YAML file,
#' the function throws an error.
#'
#' @param pathToYalmFile A string representing the file path to the YAML file.
#' @param ... Named arguments to replace placeholders in the format `<name>` within the YAML file.
#'
#' @return A parsed list representing the contents of the modified YAML file.
#'
#' @importFrom yaml yaml.load as.yaml
#'
#' @export
readAndParseYaml <- function(pathToYalmFile, ...) {
  # read the yaml file
  yalmFile <- readLines(pathToYalmFile)
  # get the names of the parameters
  names <- names(list(...))

  # check for missing placeholders
  missingParams <- names[!sapply(names, function(name) any(grepl(paste0("<", name, ">"), yalmFile)))]

  # if any placeholders are not found, throw an error
  if (length(missingParams) > 0) {
    stop(paste("Error: The following placeholders were not found in the YAML file:", paste(missingParams, collapse = ", ")))
  }

  # replace the values in the yaml file
  for (name in names) {
    yalmFile <- gsub(paste0("<", name, ">"), list(...)[[name]], yalmFile, fixed = TRUE)
  }

  # parse the yaml file
  yalmFile <- yaml::yaml.load(yalmFile)
  return(yalmFile)
}
