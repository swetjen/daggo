# s3resource_decisions

Resolved decisions:
- `BatchUploadEntityData` uses `S3_ENTITIES_BUCKET_NAME` when configured; otherwise it falls back to the primary bucket unless `S3_ENTITIES_BUCKET_REQUIRED=true`.
- ACL remains unset by default; server-side encryption is configurable via `S3_SERVER_SIDE_ENCRYPTION` (`SSE-S3`/`AES256` or `SSE-KMS` with `S3_SSE_KMS_KEY_ID`).
- Bucket ensure behavior is strict: if bucket existence cannot be confirmed after create/probe attempts, the operation fails.

Open issues:
- None.
