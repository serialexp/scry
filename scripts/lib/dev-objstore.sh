# Shared loader for Scry's destructive development object-store scripts.
#
# Source this file after setting ROOT to the repository root. Call
# `load_dev_objstore <caller-name>` before touching the bucket. Callers may point
# at a different disposable S3-compatible bucket with DEV_OBJSTORE_ENV.
# Never use this helper with a bucket whose contents must be retained.

load_dev_objstore() {
  local caller="${1:-dev-objstore}"
  local default_env="$ROOT/docker/seaweedfs/.env"
  DEV_OBJSTORE_ENV="${DEV_OBJSTORE_ENV:-$default_env}"

  if [[ ! -f "$DEV_OBJSTORE_ENV" ]]; then
    echo "[$caller] $DEV_OBJSTORE_ENV missing; run scripts/dev-seaweedfs-up.sh first or set DEV_OBJSTORE_ENV" >&2
    return 2
  fi

  set -a
  # shellcheck disable=SC1090
  source "$DEV_OBJSTORE_ENV"
  set +a

  local required
  for required in \
    SCRY_OBJSTORE_ENDPOINT \
    SCRY_OBJSTORE_REGION \
    SCRY_OBJSTORE_BUCKET \
    SCRY_OBJSTORE_ACCESS_KEY_ID \
    SCRY_OBJSTORE_SECRET_ACCESS_KEY; do
    if [[ -z "${!required:-}" ]]; then
      echo "[$caller] $required is missing from $DEV_OBJSTORE_ENV" >&2
      return 2
    fi
  done
}

aws_dev_s3() {
  AWS_ACCESS_KEY_ID="$SCRY_OBJSTORE_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$SCRY_OBJSTORE_SECRET_ACCESS_KEY" \
  AWS_SESSION_TOKEN="${SCRY_OBJSTORE_SESSION_TOKEN:-}" \
  AWS_REGION="$SCRY_OBJSTORE_REGION" \
  AWS_DEFAULT_REGION="$SCRY_OBJSTORE_REGION" \
    aws --endpoint-url "$SCRY_OBJSTORE_ENDPOINT" "$@"
}

empty_dev_objstore_bucket() {
  local caller="${1:-dev-objstore}"
  local is_local=false
  case "$SCRY_OBJSTORE_ENDPOINT" in
    http://127.0.0.1:*|http://localhost:*|http://\[::1\]:*) is_local=true ;;
  esac
  if [[ "${ALLOW_NON_DEV_BUCKET_RESET:-0}" != "1" ]] \
      && { [[ "$SCRY_OBJSTORE_BUCKET" != "scry-dev" ]] || [[ "$is_local" != true ]]; }; then
    echo "[$caller] refusing to empty bucket '$SCRY_OBJSTORE_BUCKET' at '$SCRY_OBJSTORE_ENDPOINT'; expected local scry-dev or set ALLOW_NON_DEV_BUCKET_RESET=1 explicitly" >&2
    return 2
  fi
  aws_dev_s3 s3 rm "s3://$SCRY_OBJSTORE_BUCKET/" --recursive >/dev/null
  local remaining
  remaining="$(aws_dev_s3 s3api list-objects-v2 --bucket "$SCRY_OBJSTORE_BUCKET" --max-keys 1 --query 'KeyCount' --output text)"
  if [[ "$remaining" != "0" && "$remaining" != "None" ]]; then
    echo "[$caller] bucket reset left objects behind (KeyCount=$remaining)" >&2
    return 1
  fi
}
