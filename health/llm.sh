#!/bin/bash

###############################################################################
### WARNING: If this script returns non-zero for a service, SAIA will cancel it
### Proceed with caution
###############################################################################

HOSTPORT="$1" # Pass in host:port
SERVICE="$2" # Pass in service (model) id

# Run curl command
RESPONSE_CODE=$(curl -o /dev/null -w "%{http_code}" "$HOSTPORT"/v1/completions -H 'Accept: application/json'  -H 'Content-Type: application/json' --max-time 180 -d @- <<EOF
{
  "model": "${SERVICE}",
  "prompt": "San",
  "max_tokens": 1,
  "temperature": 0
}
EOF
)

if [ "$RESPONSE_CODE" -eq 200 ]; then
    exit 0
else
    exit 1
fi