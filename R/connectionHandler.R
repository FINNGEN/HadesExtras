

#' Create a ConnectionHandler from a configuration list
#'
#' @param configConnection A list containing connection configuration settings:
#'   - tempEmulationSchema: Schema to use for temp table emulation
#'   - connectionDetailsSettings: Settings for creating connection details
#'
#' @return A ConnectionHandler object
#'
#' @importFrom checkmate assertList assertNames
#' @importFrom rlang exec
#' @importFrom DatabaseConnector createDbiConnectionDetails createConnectionDetails
#' @importFrom ResultModelManager ConnectionHandler
#'
#' @export

connectionHandlerFromList <- function(configConnection) {
  # check parameters
  configConnection |> checkmate::assertList()
  configConnection |>
    names() |>
    checkmate::assertNames(must.include = c( "connectionDetailsSettings"))

  # set tempEmulationSchema if in config
  if (!is.null(configConnection$tempEmulationSchema)) {
    options(sqlRenderTempEmulationSchema = configConnection$tempEmulationSchema)
  } else {
    options(sqlRenderTempEmulationSchema = NULL)
  }

  # create connectionHandler
  connectionDetailsSettings <- configConnection$connectionDetailsSettings
  if (!is.null(connectionDetailsSettings$drv)) {
    # IBD connection details
    eval(parse(text = paste0("tmpDriverVar <- ", connectionDetailsSettings$drv)))
    connectionDetailsSettings$drv  <- tmpDriverVar
    connectionDetails <- rlang::exec(DatabaseConnector::createDbiConnectionDetails, !!!connectionDetailsSettings)
  } else {
    # JDBC connection details
    connectionDetails <- rlang::exec(DatabaseConnector::createConnectionDetails, !!!connectionDetailsSettings)
  }

  connectionHandler <- ResultModelManager::ConnectionHandler$new(
    connectionDetails = connectionDetails,
    loadConnection = FALSE
  )

  return(connectionHandler)

}
