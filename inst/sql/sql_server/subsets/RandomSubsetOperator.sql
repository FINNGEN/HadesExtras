SELECT
  target.subject_id,
  target.cohort_start_date,
  target.cohort_end_date
{@output_table != ''} ? {INTO @output_table}
FROM @target_table target
INNER JOIN (
  SELECT subject_id
  FROM (
    SELECT
      subject_id,
      ROW_NUMBER() OVER (
        ORDER BY
          ABS((
            (ABS(CAST(subject_id AS BIGINT)) % 2147483647) * 1103515245
            + @random_seed
          ) % 2147483647),
          subject_id
      ) AS row_num
    FROM (
      SELECT DISTINCT subject_id
      FROM @target_table
    ) subjects
  ) ranked_subjects
  WHERE row_num <= @number_of_subjects
) selected_subjects
  ON target.subject_id = selected_subjects.subject_id
