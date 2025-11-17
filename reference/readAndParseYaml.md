# Read and Parse YAML File with Placeholder Replacement

Reads a YAML file from a given path, replaces specified placeholders
with provided values, and returns the parsed content. If any provided
placeholders are not found in the YAML file, the function throws an
error.

## Usage

``` r
readAndParseYaml(pathToYalmFile, ...)
```

## Arguments

- pathToYalmFile:

  A string representing the file path to the YAML file.

- ...:

  Named arguments to replace placeholders in the format `<name>` within
  the YAML file.

## Value

A parsed list representing the contents of the modified YAML file.
