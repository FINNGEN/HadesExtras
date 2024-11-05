#' tmp fix for DatabaseConnector::inDatabaseSchema
#'
#' till this is fixed https://github.com/OHDSI/DatabaseConnector/issues/236
#'
#' @param databaseSchema The name of the database schema.
#' @param table The name of the table.
#'
#' @importFrom dbplyr in_schema
#'
#' @return The fully qualified table name with the database schema.
#'
#' @export
tmp_inDatabaseSchema <- function(databaseSchema, table) {
  return(dbplyr::in_schema(databaseSchema, table))
}

