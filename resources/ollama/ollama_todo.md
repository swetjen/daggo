# ollama_decisions

Resolved decisions:
- Retry policy is configurable per call via `QueryOptions.RetryDelays` (resource defaults remain in place).
- Empty response content is treated as success by default for parity, with opt-in strict behavior via `QueryOptions.TreatEmptyAsError=true`.
- Authorization supports non-Bearer schemes via `QueryOptions.AuthHeader` or `OLLAMA_AUTH_HEADER`; otherwise it falls back to `Bearer $OLLAMA_API_KEY`.

Open issues:
- None.
