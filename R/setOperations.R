
#' Split a string into components based on specified patterns
#'
#' This function splits a string into components based on the presence of certain patterns,
#' such as digits, operators, and parentheses.
#' The valid operators are: Upd, Ip, Mp
#'
#' @param string The input string to be split.
#' @return A character vector containing the split components.
#'
splitString <- function(string) {
  # valid operators
  operators <- c("Upd", "Ip", "Mp")

  # remove spaces
  string <- gsub(" ", "", string)

  # split the string into a list of characters un less they are between parentheses
  splitString <-unlist(strsplit(string, "(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)|(?<=\\()|(?=\\()|(?<=\\))|(?=\\))", perl = TRUE))

  wrongTokens <- c()
  for (i in 1:length(splitString)) {
    if (!(splitString[i] %in% operators) && !grepl("^\\d+$", splitString[i]) && splitString[i] != "(" && splitString[i] != ")") {
      wrongTokens <- c(wrongTokens, splitString[i])
    }
  }

  if (length(wrongTokens) > 0) {
    stop(paste("Invalid tokens:", paste(wrongTokens, collapse = ", ")))
  }

  # join these strings bits if they are between parenthesis
  splitStringJoinedParenthesis <- c()
  nOpenParenthesis <- 0
  buffer <- c()
  for (i in 1:length(splitString)) {

    if (splitString[i] == "(") {
      nOpenParenthesis <- nOpenParenthesis + 1
    }

    if(nOpenParenthesis > 0){
      buffer <- c(buffer, splitString[i])
    }else{
      splitStringJoinedParenthesis <- c(splitStringJoinedParenthesis, paste0(buffer, collapse = ""), splitString[i])
      buffer <- c()
    }

    if (splitString[i] == ")") {
      nOpenParenthesis <- nOpenParenthesis - 1
    }

  }
  # if buffer still full append, (typically bcs a close parenthesis at the end)
  if(length(buffer)!=0){
    splitStringJoinedParenthesis <- c(splitStringJoinedParenthesis, paste0(buffer, collapse = ""))
  }

  splitStringJoinedParenthesis <- splitStringJoinedParenthesis[setdiff(1:length(splitStringJoinedParenthesis),which(splitStringJoinedParenthesis==""))]

  if (nOpenParenthesis !=0 ) {
    stop(" Missing closing brackets")
  }

  return(splitStringJoinedParenthesis)
}

#' Convert an operation string to a binary tree
#'
#' This function converts a string representing an operation into a binary tree structure.
#' The operation string should contain operators ('Upd', 'Ip', 'Mp'), numbers, and parentheses.
#' The function recursively builds the binary tree based on operator priorities.
#'
#' @param operationString The input string representing an operation.
#' @return A binary tree structure representing the operation.
#' @examples
#' operationStringToBinaryTree("(1Upd(2Ip3))Mp4")
#' # Returns: $left
#' #         : 1
#' #         $operation
#' #         : "Upd"
#' #         $right
#' #         : $left
#' #         :   2
#' #         :   $operation
#' #         :   "Ip"
#' #         :   $right
#' #         :   3
#' #         $right
#' #         : 4
operationStringToBinaryTree <- function(operationString) {
  # valid operators
  operators <- c("Upd", "Ip", "Mp")

  # remove the firsts and last parenthesis, if they are teh complementary to each other
  if (substr(operationString, 1, 1) == "(" && substr(operationString, nchar(operationString), nchar(operationString)) == ")") {
    operationStringNoParenthesis <- substr(operationString, 2, nchar(operationString) - 1)
    openingParenthesis <- which(strsplit(operationStringNoParenthesis, "")[[1]] == "(")
    closingParenthesis <- which(strsplit(operationStringNoParenthesis, "")[[1]] == ")")
    if (length(openingParenthesis) != length(closingParenthesis)) {
      stop("Missing closing parenthesis")
    }
    if (all(closingParenthesis - openingParenthesis > 0)) {
      operationString <- operationStringNoParenthesis
    }
  }

  # split the string into a list of operators, numbers, or string with in parethesis
  splitString <- splitString(operationString)


  # if the list has only one element
  if (length(splitString) == 1) {
    if (is.na(suppressWarnings(as.integer(splitString)))){
      # and is not a number, it was in parenthesis, run as operation
      return(operationStringToBinaryTree(splitString))
    }else{
      # and is a number, return as number
      return(as.integer(splitString))
    }
  }

  # if the list has two elements, there si some error
  if (length(splitString) == 2) {
    stop("Operations must have atleast 3 elements")
  }

  # if the list has more than two elements, find the operator, priority is the left operator
  # and split the list into two lists, one for the left side of the operator and one for the right side
  if (length(splitString) > 2) {
    # set priority of operators. We leave this, priority is from left to right
    # if ("Upd" %in% splitString) {
    #   operator <- "Upd"
    # } else if ("Ip" %in% splitString) {
    #   operator <- "Ip"
    # } else {
    #   operator <- "Mp"
    # }
    # operatorIndex <- min(which(splitString == operator))
    operatorIndex <- which(splitString %in% operators)[1]
    operator <- splitString[operatorIndex]

    leftSide <- splitString[1:(operatorIndex - 1)]
    rightSide <- splitString[(operatorIndex + 1):length(splitString)]

    return(list(
      left = operationStringToBinaryTree(paste(leftSide, collapse = "")),
      operation = operator,
      right = operationStringToBinaryTree(paste(rightSide, collapse = "")))
    )
  }
}


binaryTreeToSQL <- function(binaryTree, side, depth) {

  side |> checkmate::assertString()
  side |> checkmate::assertSubset(c("left", "right", "root"))
  depth |> checkmate::assertInteger()

  positionId <- paste0(side, depth)
  position <- paste0("depth = ", depth, "; side = ", side, "; positionId = ", positionId)
  tabs <- paste(rep("  ", depth+2), collapse = "")

  # if leaf, return the sql that reads the cohort table with the number as cohort_id
  if (is.numeric(binaryTree)) {
    sql  <- paste0(
      tabs, "-- ", position, "\n",
      tabs, "SELECT subject_id, cohort_start_date, cohort_end_date FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id = ", binaryTree , "\n"
      )
    return(list(
      sql = sql,
      positionId = positionId
    ))
  }

  # if not leaf, check if the binary tree has the correct structure
  binaryTree |> checkmate::assertList()
  binaryTree |> names() |>  checkmate::assertSubset(c("left", "operation", "right"))

  left <- binaryTreeToSQL(binaryTree$left, "left", depth + 1L)
  right <- binaryTreeToSQL(binaryTree$right, "right", depth + 1L)

  # operations
  if (binaryTree$operation == "Upd") {
    # Ip
    sql  <- paste0(
      tabs, "-- Operation Ip: ", position, "\n",
      tabs, "SELECT \n",
      tabs, "   subject_id AS subject_id, \n",
      tabs, "   MIN(cohort_start_date) AS cohort_start_date, \n",
      tabs, "   MAX(cohort_end_date) AS cohort_end_date \n",
      tabs, "FROM \n",
      tabs, "(\n",
      left$sql,
      tabs, "UNION ALL\n",
      right$sql,
      tabs, ")\n",
      tabs, "GROUP BY subject_id \n"
    )

  } else if (binaryTree$operation == "Ip") {
    # Ip
    sql  <- paste0(
      tabs, "-- Operation Ip: ", position, "\n",
      tabs, "SELECT ", positionId, "left.* FROM \n",
      tabs, "(\n",
      left$sql,
      tabs, ") AS ", positionId, "left\n",
      tabs, "INNER JOIN\n",
      tabs, "(\n",
      right$sql,
      tabs, ") AS ", positionId, "right\n",
      tabs, "ON ", positionId, "left.subject_id = ", positionId, "right.subject_id \n"
    )

  } else {
    # Mp
    sql  <- paste0(
      tabs, "-- Operation Mp: ", position, "\n",
      tabs, "SELECT ", positionId, "left.* FROM \n",
      tabs, "(\n",
      left$sql,
      tabs, ") AS ", positionId, "left\n",
      tabs, "LEFT JOIN\n",
      tabs, "(\n",
      right$sql,
      tabs, ") AS ", positionId, "right\n",
      tabs, "ON ", positionId, "left.subject_id = ", positionId, "right.subject_id \n",
      tabs, "WHERE ", positionId, "right.subject_id IS NULL \n"
    )
  }


  return(list(
    sql = sql,
    positionId = positionId
  ))

}




operationStringToSQL <- function(operationString) {
  # convert the operation string to a binary tree
  binaryTree <- operationStringToBinaryTree(operationString)

  res <- binaryTreeToSQL(binaryTree, "root", 0L)

  sql <- paste0(
    "SELECT\n",
    "subject_id,\n",
    "cohort_start_date,\n",
    "cohort_end_date\n",
    "{@output_table != ''} ? {INTO @output_table}\n",
    "FROM(\n",
    "  SELECT\n",
    "  *,\n",
    "  ROW_NUMBER() OVER (PARTITION BY subject_id, cohort_start_date, cohort_end_date)  repeated\n",
    "  FROM(\n",
    res$sql,
    "  )\n",
    ")\n",
    "WHERE repeated = 1\n"
  )

  # convert the binary tree to a SQL string
  return(sql)
}



cohortDataToCohortDefinitionSet <- function(
    cohortData,
    cohortIdOffset = 0L,
    skipCohortDataCheck = FALSE
){

  #
  # Validate parameters
  #
  checkmate::assertInt(cohortIdOffset)
  checkmate::assertLogical(skipCohortDataCheck)

  if(skipCohortDataCheck == TRUE){
    assertCohortData(cohortData)
  }

  #
  # Function
  #
  sqlToRender <- SqlRender::readSql(system.file("sql/sql_server/ImportCohortTable.sql", package = "HadesExtras", mustWork = TRUE))

  cohortDefinitionSet <- cohortData |>
    tidyr::nest(.key = "cohort", .by = c("cohort_name")) |>
    dplyr::transmute(
      cohortId = as.double(dplyr::row_number()+cohortIdOffset),
      cohortName = cohort_name,
      json = purrr::map_chr(.x = cohort, .f=.cohortDataToJson),
      sql = purrr::map2_chr(
        .x = cohortId,
        .y = cohort,
        .f=~{paste0("--",  digest::digest(.y), "\n",
                    SqlRender::render(
                      sql = sqlToRender,
                      source_cohort_table = getOption("cohortDataImportTmpTableName", "tmp_cohortdata"),
                      source_cohort_id = .x,
                      is_temp_table = TRUE
                    ))}
      )
    )

  return(cohortDefinitionSet)
}

