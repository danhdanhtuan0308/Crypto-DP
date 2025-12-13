# Dashboard AI (DeepSeek)

This dashboard includes an optional AI sidebar panel that answers questions using the latest 1-minute data loaded from GCS.

## Environment variables

- `DEEPSEEK_API_KEY` (required)
- `DEEPSEEK_BASE_URL` (optional, default `https://api.deepseek.com`)
- `DEEPSEEK_MODEL` (optional, default `deepseek-chat`)

GCS credentials are already used by the dashboard:
- `GCP_SERVICE_ACCOUNT_JSON` (recommended)

## Notes

- The AI only uses data provided in the request context (latest rows from the dashboard’s GCS-loaded dataframe).
- When the AI panel is open, auto-refresh is paused to avoid interrupting text input.
