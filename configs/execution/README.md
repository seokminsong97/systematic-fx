# Execution Configurations

Each file will version routing delay, market/limit fill behavior, stop trigger
and fill semantics, partial fills, liquidity consumption, and OCO ordering.

`execution_pending_v1.toml` freezes non-negotiable safety semantics while
leaving unknown numeric and order-policy inputs explicitly unresolved. It
blocks economic screening and cannot be used as a backtest execution model.
