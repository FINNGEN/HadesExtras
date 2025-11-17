# Generate Style for Bar

Generates a CSS style for a bar based on the percentage values of male,
female, and NA.

## Usage

``` r
.barStyle(
  perSexStr,
  colorSexMale = "#2c5e77",
  colorSexFemale = "#BF616A",
  colorSexNa = "#8C8C8C"
)
```

## Arguments

- perSexStr:

  A string representing the percentages of male, female, and NA (e.g.,
  "40% 30% 30%").

- colorSexMale:

  The color for the male section (default is "#2c5e77").

- colorSexFemale:

  The color for the female section (default is "#BF616A").

- colorSexNa:

  The color for the NA section (default is "#8C8C8C").

## Value

A list containing CSS style properties for the bar.
