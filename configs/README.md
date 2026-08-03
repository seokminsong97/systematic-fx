# Versioned Research Configuration

This directory owns reviewable, versioned research inputs. Machine paths and
database credentials belong in environment variables, not here.

- `campaigns/`: immutable calendar scope, split identity, and trial budget
- `costs/`: commissions, fees, slippage, and fixed-cost allocation versions
- `execution/`: latency, fill, stop-trigger, and OCO model versions
- `features/`: one-second and five-minute feature-definition versions

Do not edit a configuration after a run references its checksum. Create a new
version instead.
