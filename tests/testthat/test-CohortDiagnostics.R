
test_that("CohortDiagnostics_mergeCsvResults merges CSV results correctly", {

  temp_dir <- tempdir()
  result_folder1 <- file.path(temp_dir, "result_folder1")
  result_folder2 <- file.path(temp_dir, "result_folder2")
  merged_results_folder <- file.path(temp_dir, "merged_results")

  dir.create(result_folder1)
  dir.create(result_folder2)
  dir.create(merged_results_folder)

  # Create some dummy CSV files in result folders
  cat(c("Header1,Header2,Header3",
        "folder1file1data1,folder1file1data2,folder1file1data3",
        "folder1file1data4,folder1file1data5,folder1file1data6"
  ), file = file.path(result_folder1, "file1.csv"),sep = "\n")
  cat(c("Header1,Header2,Header3",
        "folder1file2data1,folder1file2data2,folder1file2data3",
        "folder1file2data4,folder1file2data5,folder1file2data6"
  ), file = file.path(result_folder1, "file2.csv"),sep = "\n")
  cat(c("Header1,Header2,Header3",
        "folder2file1data1,folder2file1data2,folder2file1data3",
        "folder2file1data4,folder2file1data5,folder2file1data6"
  ), file = file.path(result_folder2, "file1.csv"),sep = "\n")
  cat(c("Header1,Header2,Header3",
        "folder2file2data1,folder2file2data2,folder2file2data3",
        "folder2file2data4,folder2file2data5,folder2file2data6"
  ), file = file.path(result_folder2, "file2.csv"),sep = "\n")



  # Call the function
  merged_folder <- CohortDiagnostics_mergeCsvResults(
    c(result_folder1, result_folder2),
    merged_results_folder
  )

  # Check if the merged results folder exists
  expect_true(dir.exists(merged_folder))

  # Check if the merged CSV files exist and have correct content
  expect_true(file.exists(file.path(merged_folder, "file1.csv")))
  expect_true(file.exists(file.path(merged_folder, "file2.csv")))

  merged_file1_content <- readLines(file.path(merged_folder, "file1.csv"))
  merged_file2_content <- readLines(file.path(merged_folder, "file2.csv"))

  merged_file1_content |> expect_equal(
    c("Header1,Header2,Header3",
      "folder1file1data1,folder1file1data2,folder1file1data3",
      "folder1file1data4,folder1file1data5,folder1file1data6",
      "folder2file1data1,folder2file1data2,folder2file1data3",
      "folder2file1data4,folder2file1data5,folder2file1data6")
  )
  merged_file2_content |> expect_equal(
    c("Header1,Header2,Header3",
      "folder1file2data1,folder1file2data2,folder1file2data3",
      "folder1file2data4,folder1file2data5,folder1file2data6",
      "folder2file2data1,folder2file2data2,folder2file2data3",
      "folder2file2data4,folder2file2data5,folder2file2data6")
  )

  # Clean up
  unlink(temp_dir, recursive = TRUE)
})
