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

echo "DIASPORA_TEST_BACKEND: ${DIASPORA_TEST_BACKEND}"
echo "DIASPORA_TEST_BACKEND_ARGS: ${DIASPORA_TEST_BACKEND_ARGS}"
echo "DIASPORA_TEST_TOPIC_ARGS: ${DIASPORA_TEST_TOPIC_ARGS}"
echo "BEFORE_COMMAND: ${BEFORE_COMMAND}"
echo "AFTER_COMMAND: ${AFTER_COMMAND}"

# Binary tests
for test_file in ${SCRIPT_DIR}/Diaspora*Test ; do
    echo "Running test file ${test_file}"
    if [ -n "$BEFORE_COMMAND" ]; then
        eval "$BEFORE_COMMAND"
        if [ "$?" -ne 0 ]; then
            RET=1
        fi
    fi
    timeout 60s ${test_file}
    r=$?
    if [ "$r" -ne 0 ]; then
        RET=1
    fi
    if [ -n "$AFTER_COMMAND" ]; then
        eval "$AFTER_COMMAND $r"
        if [ "$?" -ne 0 ]; then
            RET=1
        fi
    fi
done

# Python tests
# Discover all tests
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$SCRIPT_DIR/../lib \
DIASPORA_STREAM_PATH=$(python -c \
    "import diaspora_stream, os; print(os.path.dirname(os.path.abspath(diaspora_stream.__file__)))")

# Run each test individually
for test_file in $DIASPORA_STREAM_PATH/test_*.py; do
    echo "------------------------------------------------------------"
    echo "Running test: $test_file"
    echo "------------------------------------------------------------"
    filename=$(basename $test_file)
    test_name="diaspora_stream.${filename%.*}"
    if [ -n "$BEFORE_COMMAND" ]; then
        eval "$BEFORE_COMMAND"
        if [ "$?" -ne 0 ]; then
            RET=1
        fi
    fi
    LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$SCRIPT_DIR/../lib \
    timeout 60s python -m unittest -v $test_name
    r=$?
    if [ "$r" -ne 0 ]; then
        RET=1
    fi
    if [ -n "$AFTER_COMMAND" ]; then
        eval "$AFTER_COMMAND $r"
        if [ "$?" -ne 0 ]; then
            RET=1
        fi
    fi
done

exit $RET
