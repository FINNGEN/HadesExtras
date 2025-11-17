# reactable_connectionStatus

A function to display connection status in a reactable.

## Usage

``` r
reactable_connectionStatus(
  connectionStatus,
  emojis = list(error = "❌", warning = "⚠️", success = "✅", info = "ℹ️")
)
```

## Arguments

- connectionStatus:

  A data frame containing connection status information.

- emojis:

  list of emojis

## Value

A reactable displaying the connection status information.
