#!/bin/sh

export PUBSUB_EMULATOR_HOST=localhost:${PUBSUB_PORT}
echo "starting pubsub emulator on port ${PUBSUB_PORT}"
gcloud beta emulators pubsub start --host-port=0.0.0.0:${PUBSUB_PORT} --log-http --verbosity=debug --user-output-enabled "$@"