# multidb_todo

- Clarify final DSN-to-driver mapping. Current implementation defaults to `sqlite` unless overridden.
- Confirm whether requesting a second DSN key after first initialization should log/warn or remain fully silent.
- Decide connection lifecycle ownership for production: singleton close behavior, health checks, and reconnect strategy.

