#' Convert Operation String to SQL Query
#'
#' Converts an operation string into an SQL query that can be executed against a database.
#' This function first converts the operation string into a binary tree representation and then
#' generates the corresponding SQL query.
#'
#' @param operationString The input string representing an operation to be converted into SQL.
#'
#' @return An SQL query string derived from the input operation string.
#'
#' @export
operationStringToSQL <- function(operationString) {
  # convert the operation string to a binary tree
  binaryTree <- .operationStringToBinaryTree(operationString)

  res <- ..binaryTreeToSQL(binaryTree, "root", 0L)

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


#' Convert Binary Tree to SQL Query
#'
#' Converts a binary tree representation of an operation into SQL query statements.
#' This function is useful for generating SQL queries from binary tree structures.
#'
#' @param binaryTree A list representing the binary tree structure of an operation.
#'        The list should contain elements 'left', 'operation', and 'right'.
#' @param side A string indicating the side of the binary tree node ('left', 'right', 'root').
#' @param depth An integer indicating the depth of the current node in the binary tree.
#'
#' @return A list containing the SQL query statement and the position ID of the current node.
#'
#' @importFrom checkmate assertString assertSubset assertInteger
#'
..binaryTreeToSQL <- function(binaryTree, side, depth) {

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

  left <- ..binaryTreeToSQL(binaryTree$left, "left", depth + 1L)
  right <- ..binaryTreeToSQL(binaryTree$right, "right", depth + 1L)

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


#' Convert Operation String to Binary Tree Representation
#'
#' Converts an operation string into a binary tree representation, where valid operators include "Upd", "Ip", and "Mp".
#' This function is useful for parsing operation strings and constructing binary trees for evaluation.
#'
#' @param operationString The input string representing an operation to be converted into a binary tree.
#'
#' @return A list representing the binary tree structure of the operation string.
#'
.operationStringToBinaryTree<- function(operationString) {
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
      return(.operationStringToBinaryTree(splitString))
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
      left = .operationStringToBinaryTree(paste(leftSide, collapse = "")),
      operation = operator,
      right = .operationStringToBinaryTree(paste(rightSide, collapse = "")))
    )
  }
}




#' Split String into Valid Tokens
#'
#' Splits a string into valid tokens based on certain rules and operators. Valid tokens include "Upd", "Ip", "Mp",
#' numeric values, and parentheses. This function is useful for parsing strings containing a cohort operation.
#'
#' @param string The input string to be split into tokens.
#'
#' @return A character vector containing valid tokens extracted from the input string.
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



