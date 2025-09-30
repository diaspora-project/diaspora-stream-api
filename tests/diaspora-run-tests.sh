#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

BEFORE_COMMAND=""
AFTER_COMMAND=""
RET=0

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

# Binary tests
for test_file in ${SCRIPT_DIR}/Diaspora*Test ; do
    echo "Running test file ${test_file}"
    if [ -n "$BEFORE_COMMAND" ]; then
        eval "$BEFORE_COMMAND"
        if [ "$?" -ne 0 ]; then
            RET=1
        fi
    fi
    ${test_file}
    if [ "$?" -ne 0 ]; then
        RET=1
    fi
    if [ -n "$AFTER_COMMAND" ]; then
        eval "$AFTER_COMMAND"
        if [ "$?" -ne 0 ]; then
            RET=1
        fi
    fi
done

# Python tests
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$SCRIPT_DIR/../lib \
    python -m unittest discover diaspora_stream
if [ "$?" -ne 0 ]; then
    RET=1
fi

exit $RET
