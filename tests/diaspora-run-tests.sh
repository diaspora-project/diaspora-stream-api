#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

export DIASPORA_TEST_BACKEND="simple"
BEFORE_COMMAND=""
AFTER_COMMAND=""

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --backend-args)
        export DIASPORA_TEST_BACKEND_ARGS="$2"
        shift # past argument
        shift # past value
        ;;
        --topic-args)
        export DIASPORA_TEST_TOPIC_ARGS="$2"
        shift # past argument
        shift # past value
        ;;
        --before)
        BEFORE_COMMAND="$2"
        shift # past argument
        shift # past value
        ;;
        --after)
        AFTER_COMMAND="$2"
        shift # past argument
        shift # past value
        ;;
        *)    # unknown option
        export DIASPORA_TEST_BACKEND="$1"
        shift # past argument
        ;;
    esac
done

for test_file in ${SCRIPT_DIR}/Diaspora*Test ; do
    echo "Running test file ${test_file}"
    if [ -n "$BEFORE_COMMAND" ]; then
        eval "$BEFORE_COMMAND"
    fi
    ${test_file}
    if [ -n "$AFTER_COMMAND" ]; then
        eval "$AFTER_COMMAND"
    fi
done
