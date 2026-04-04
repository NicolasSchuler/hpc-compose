#!/bin/sh
set -eu

: "${LLM_BASE_URL:=http://127.0.0.1:8080}"
: "${LLM_REQUEST_FILE:=/workflow/request.json}"

echo "Sending test request to ${LLM_BASE_URL}/v1/chat/completions"
if [ ! -f "${LLM_REQUEST_FILE}" ]; then
  echo "request payload file is missing: ${LLM_REQUEST_FILE}" >&2
  exit 1
fi

curl --fail --silent --show-error \
  -H "Content-Type: application/json" \
  "${LLM_BASE_URL}/v1/chat/completions" \
  --data @"${LLM_REQUEST_FILE}"
printf '\n'
