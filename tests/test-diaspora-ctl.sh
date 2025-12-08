#!/usr/bin/env bash

# Test suite for diaspora-ctl with files driver
# This script tests the diaspora-ctl executable functionality

# set -e  # Exit on error

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
BUILD_DIR="${SCRIPT_DIR}/.."
DIASPORA_CTL="${BUILD_DIR}/bin/diaspora-ctl"
TEST_DATA_DIR="${SCRIPT_DIR}/diaspora-ctl-test-data"

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Setup test environment
setup() {
    echo "================================================"
    echo "Setting up test environment"
    echo "================================================"

    # Check if diaspora-ctl exists
    if [ ! -f "$DIASPORA_CTL" ]; then
        echo -e "${RED}ERROR: diaspora-ctl not found at $DIASPORA_CTL${NC}"
        echo "Please build the project first"
        exit 1
    fi

    # Create and clean test data directory
    mkdir -p "$TEST_DATA_DIR"
    rm -rf "${TEST_DATA_DIR:?}"/*

    echo "Test data directory: $TEST_DATA_DIR"
    echo "diaspora-ctl: $DIASPORA_CTL"
    echo ""
}

# Cleanup test environment
cleanup() {
    echo ""
    echo "================================================"
    echo "Cleaning up test environment"
    echo "================================================"
    rm -rf "${TEST_DATA_DIR:?}"
    echo "Cleanup complete"
}

# Print test result
print_result() {
    local test_name=$1
    local result=$2

    TESTS_RUN=$((TESTS_RUN + 1))

    if [ "$result" -eq 0 ]; then
        echo -e "${GREEN}✓ PASS${NC}: $test_name"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        echo -e "${RED}✗ FAIL${NC}: $test_name"
        TESTS_FAILED=$((TESTS_FAILED + 1))
    fi
}

# Print error message in red
print_error() {
    echo -e "${RED}  ERROR: $1${NC}"
}

# Print info message in yellow
print_info() {
    echo -e "${YELLOW}  INFO: $1${NC}"
}

# Test: Display help
test_help() {
    echo ""
    echo "Test: Display help"
    echo "-------------------"

    local output
    output=$("$DIASPORA_CTL" --help 2>&1)
    local result=$?

    if [ $result -ne 0 ]; then
        print_error "diaspora-ctl --help returned non-zero exit code: $result"
        print_info "Output: $output"
    fi

    print_result "diaspora-ctl --help" $result
}

# Test: Create a simple topic
test_create_simple_topic() {
    echo ""
    echo "Test: Create a simple topic"
    echo "----------------------------"

    local root_path="${TEST_DATA_DIR}/simple"
    mkdir -p "$root_path"

    local output
    output=$("$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "test-topic-1" \
        2>&1)

    local result=$?

    if [ $result -ne 0 ]; then
        print_error "topic create command failed with exit code: $result"
        print_info "Output: $output"
        result=1
    elif [ ! -d "$root_path/test-topic-1" ]; then
        print_error "Topic directory not created at $root_path/test-topic-1"
        print_info "Directory contents: $(ls -la "$root_path" 2>&1)"
        result=1
    else
        result=0
    fi

    print_result "Create simple topic" $result
}

# Test: Create topic with multiple partitions
test_create_topic_with_partitions() {
    echo ""
    echo "Test: Create topic with multiple partitions"
    echo "---------------------------------------------"

    local root_path="${TEST_DATA_DIR}/partitions"
    mkdir -p "$root_path"

    local output
    output=$("$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "multi-partition-topic" \
        --topic.num_partitions 4 \
        2>&1)

    local result=$?

    if [ $result -ne 0 ]; then
        print_error "topic create command failed with exit code: $result"
        print_info "Output: $output"
        result=1
    else
        # Verify partition directories were created (00000000, 00000001, 00000002, 00000003)
        local missing_partitions=""
        for i in 0 1 2 3; do
            local partition_dir="$root_path/multi-partition-topic/partitions/0000000$i"
            if [ ! -d "$partition_dir" ]; then
                missing_partitions="$missing_partitions 0000000$i"
            fi
        done

        if [ -n "$missing_partitions" ]; then
            print_error "Missing partition directories:$missing_partitions"
            print_info "Partitions directory contents: $(ls -la "$root_path/multi-partition-topic/partitions/" 2>&1)"
            result=1
        else
            result=0
        fi
    fi

    print_result "Create topic with 4 partitions" $result
}

# Test: Create topic with config file
test_create_topic_with_config_file() {
    echo ""
    echo "Test: Create topic with config file"
    echo "-------------------------------------"

    local root_path="${TEST_DATA_DIR}/config"
    mkdir -p "$root_path"

    # Create driver config file
    local driver_config="${TEST_DATA_DIR}/driver-config.json"
    cat > "$driver_config" << EOF
{
    "root_path": "$root_path"
}
EOF

    # Create topic config file
    local topic_config="${TEST_DATA_DIR}/topic-config.json"
    cat > "$topic_config" << EOF
{
    "num_partitions": 2
}
EOF

    local output
    output=$("$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver-config "$driver_config" \
        --name "config-topic" \
        --topic-config "$topic_config" \
        2>&1)

    local result=$?

    if [ $result -ne 0 ]; then
        print_error "topic create command failed with exit code: $result"
        print_info "Output: $output"
        result=1
    else
        # Verify topic was created with 2 partitions
        if [ ! -d "$root_path/config-topic/partitions/00000000" ]; then
            print_error "Partition 00000000 not created"
            print_info "Partitions directory: $(ls -la "$root_path/config-topic/partitions/" 2>&1)"
            result=1
        elif [ ! -d "$root_path/config-topic/partitions/00000001" ]; then
            print_error "Partition 00000001 not created"
            print_info "Partitions directory: $(ls -la "$root_path/config-topic/partitions/" 2>&1)"
            result=1
        else
            result=0
        fi
    fi

    # Cleanup config files
    rm -f "$driver_config" "$topic_config"

    print_result "Create topic with config files" $result
}

# Test: Verify component metadata files are created
test_component_metadata_files() {
    echo ""
    echo "Test: Verify component metadata files"
    echo "---------------------------------------"

    local root_path="${TEST_DATA_DIR}/components"
    mkdir -p "$root_path"

    local output
    output=$("$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "component-topic" \
        2>&1)

    local result=$?

    if [ $result -ne 0 ]; then
        print_error "topic create command failed with exit code: $result"
        print_info "Output: $output"
        result=1
    else
        result=0

        # Verify component JSON files exist
        if [ ! -f "$root_path/component-topic/validator.json" ]; then
            print_error "validator.json not found"
            print_info "Topic directory: $(ls -la "$root_path/component-topic/" 2>&1)"
            result=1
        fi

        if [ ! -f "$root_path/component-topic/serializer.json" ]; then
            print_error "serializer.json not found"
            print_info "Topic directory: $(ls -la "$root_path/component-topic/" 2>&1)"
            result=1
        fi

        if [ ! -f "$root_path/component-topic/partition-selector.json" ]; then
            print_error "partition-selector.json not found"
            print_info "Topic directory: $(ls -la "$root_path/component-topic/" 2>&1)"
            result=1
        fi

        # Verify JSON files are valid JSON
        if [ $result -eq 0 ]; then
            if ! python3 -c "import json; json.load(open('$root_path/component-topic/validator.json'))" 2>/dev/null; then
                print_error "validator.json is not valid JSON"
                print_info "Content: $(cat "$root_path/component-topic/validator.json" 2>&1)"
                result=1
            fi
            if ! python3 -c "import json; json.load(open('$root_path/component-topic/serializer.json'))" 2>/dev/null; then
                print_error "serializer.json is not valid JSON"
                print_info "Content: $(cat "$root_path/component-topic/serializer.json" 2>&1)"
                result=1
            fi
            if ! python3 -c "import json; json.load(open('$root_path/component-topic/partition-selector.json'))" 2>/dev/null; then
                print_error "partition-selector.json is not valid JSON"
                print_info "Content: $(cat "$root_path/component-topic/partition-selector.json" 2>&1)"
                result=1
            fi
        fi
    fi

    print_result "Component metadata files created and valid" $result
}

# Test: Create topic with nested metadata parameters
test_nested_metadata_parameters() {
    echo ""
    echo "Test: Create topic with nested metadata"
    echo "-----------------------------------------"

    local root_path="${TEST_DATA_DIR}/nested"
    mkdir -p "$root_path"

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "nested-topic" \
        --topic.config.option1 "value1" \
        --topic.config.option2 "value2" \
        > /dev/null 2>&1

    local result=$?

    # Just verify the topic was created
    if [ -d "$root_path/nested-topic" ]; then
        result=0
    else
        result=1
    fi

    print_result "Create topic with nested metadata parameters" $result
}

# Test: Error handling - missing required arguments
test_missing_required_args() {
    echo ""
    echo "Test: Error handling - missing required arguments"
    echo "---------------------------------------------------"

    # Should fail without --driver
    "$DIASPORA_CTL" topic create --name "test" > /dev/null 2>&1
    local result1=$?

    # Should fail without --name
    "$DIASPORA_CTL" topic create --driver "files" > /dev/null 2>&1
    local result2=$?

    # Both should fail (non-zero exit code)
    if [ $result1 -ne 0 ] && [ $result2 -ne 0 ]; then
        result=0
    else
        result=1
    fi

    print_result "Missing required arguments detected" $result
}

# Test: Error handling - duplicate topic
test_duplicate_topic() {
    echo ""
    echo "Test: Error handling - duplicate topic"
    echo "----------------------------------------"

    local root_path="${TEST_DATA_DIR}/duplicate"
    mkdir -p "$root_path"

    # Create first topic (should succeed)
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "duplicate-topic" \
        > /dev/null 2>&1

    # Try to create same topic again (should fail)
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "duplicate-topic" \
        > /dev/null 2>&1

    local result=$?

    # Should fail (non-zero exit code)
    if [ $result -ne 0 ]; then
        result=0
    else
        result=1
    fi

    print_result "Duplicate topic creation rejected" $result
}

# Test: Create multiple topics in same driver
test_multiple_topics() {
    echo ""
    echo "Test: Create multiple topics"
    echo "-----------------------------"

    local root_path="${TEST_DATA_DIR}/multiple"
    mkdir -p "$root_path"

    # Create multiple topics
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "topic-a" \
        > /dev/null 2>&1

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "topic-b" \
        > /dev/null 2>&1

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "topic-c" \
        > /dev/null 2>&1

    local result=0

    # Verify all topics exist
    if [ ! -d "$root_path/topic-a" ]; then
        result=1
    fi
    if [ ! -d "$root_path/topic-b" ]; then
        result=1
    fi
    if [ ! -d "$root_path/topic-c" ]; then
        result=1
    fi

    print_result "Create multiple topics" $result
}

# Test: List topics (basic)
test_list_topics_basic() {
    echo ""
    echo "Test: List topics (basic)"
    echo "--------------------------"

    local root_path="${TEST_DATA_DIR}/list-basic"
    mkdir -p "$root_path"

    # Create some topics
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "list-topic-1" \
        > /dev/null 2>&1

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "list-topic-2" \
        > /dev/null 2>&1

    # List topics
    local output=$("$DIASPORA_CTL" topic list \
        --driver "files" \
        --driver.root_path "$root_path" \
        2>/dev/null)

    local result=0

    # Check that both topics are in the output
    if ! echo "$output" | grep -q "list-topic-1"; then
        print_error "list-topic-1 not found in output"
        print_info "Output: $output"
        result=1
    fi

    if ! echo "$output" | grep -q "list-topic-2"; then
        print_error "list-topic-2 not found in output"
        print_info "Output: $output"
        result=1
    fi

    # Check that output has exactly 2 lines (one per topic)
    local line_count=$(echo "$output" | wc -l)
    if [ "$line_count" -ne 2 ]; then
        print_error "Expected 2 lines, got $line_count"
        print_info "Output: $output"
        result=1
    fi

    print_result "List topics (basic)" $result
}

# Test: List topics with verbose flag
test_list_topics_verbose() {
    echo ""
    echo "Test: List topics (verbose)"
    echo "----------------------------"

    local root_path="${TEST_DATA_DIR}/list-verbose"
    mkdir -p "$root_path"

    # Create a topic
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "verbose-topic" \
        --topic.num_partitions 3 \
        > /dev/null 2>&1

    # List topics with verbose flag
    local output=$("$DIASPORA_CTL" topic list \
        --driver "files" \
        --driver.root_path "$root_path" \
        --verbose \
        2>/dev/null)

    local result=0

    # Check that topic name is in the output
    if ! echo "$output" | grep -q "verbose-topic"; then
        print_error "verbose-topic not found in output"
        print_info "Output: $output"
        result=1
    fi

    # Check that metadata is present (should contain JSON)
    if ! echo "$output" | grep -q "{"; then
        print_error "JSON metadata not found in output"
        print_info "Output: $output"
        result=1
    fi

    # Check that num_partitions is in the metadata
    if ! echo "$output" | grep -q "num_partitions"; then
        print_error "num_partitions not found in metadata"
        print_info "Output: $output"
        result=1
    fi

    # Check that validator/serializer/partition_selector are in metadata
    if ! echo "$output" | grep -q "validator"; then
        print_error "validator not found in metadata"
        print_info "Output: $output"
        result=1
    fi

    print_result "List topics (verbose)" $result
}

# Test: List topics on empty driver
test_list_topics_empty() {
    echo ""
    echo "Test: List topics (empty driver)"
    echo "----------------------------------"

    local root_path="${TEST_DATA_DIR}/list-empty"
    mkdir -p "$root_path"

    # List topics without creating any
    local output=$("$DIASPORA_CTL" topic list \
        --driver "files" \
        --driver.root_path "$root_path" \
        2>/dev/null)

    local result=0

    # Output should be empty (no topics)
    if [ -n "$output" ]; then
        print_error "Expected empty output, got non-empty output"
        print_info "Output: $output"
        result=1
    fi

    print_result "List topics (empty driver)" $result
}

# Test: FIFO daemon basic functionality
test_fifo_daemon() {
    echo ""
    echo "Test: FIFO daemon basic functionality"
    echo "---------------------------------------"

    local root_path="${TEST_DATA_DIR}/fifo-daemon"
    mkdir -p "$root_path"

    local control_file="${TEST_DATA_DIR}/fifo-control"
    local producer_fifo="${TEST_DATA_DIR}/producer-fifo"
    local daemon_log="${TEST_DATA_DIR}/daemon-output.log"

    # Create a topic first
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "fifo-topic" \
        > /dev/null 2>&1

    # Start the daemon in the background, capturing output
    "$DIASPORA_CTL" fifo \
        --driver "files" \
        --driver.root_path "$root_path" \
        --control-file "$control_file" \
        --logging error \
        > "$daemon_log" 2>&1 &

    local daemon_pid=$!
    local result=0

    # Give the daemon time to start and create the control FIFO
    sleep 1

    # Check if daemon is still running
    if ! kill -0 "$daemon_pid" 2>/dev/null; then
        print_error "Daemon failed to start or crashed immediately"
        print_info "Daemon output:"
        if [ -f "$daemon_log" ]; then
            cat "$daemon_log" | while IFS= read -r line; do
                print_info "  $line"
            done
        else
            print_info "  No log file created"
        fi
        result=1
    else
        # Verify control FIFO was created
        if [ ! -p "$control_file" ]; then
            print_error "Control FIFO not created at $control_file"
            print_info "Directory contents: $(ls -la "$(dirname "$control_file")" 2>&1)"
            result=1
        else
            # Send a control command to create a producer
            echo "$producer_fifo -> fifo-topic" > "$control_file" 2>/dev/null || true

            # Give daemon time to process the command and create the producer FIFO
            sleep 1

            # Verify producer FIFO was created
            if [ ! -p "$producer_fifo" ]; then
                print_error "Producer FIFO not created at $producer_fifo"
                print_info "Directory contents: $(ls -la "$(dirname "$producer_fifo")" 2>&1)"
                result=1
            else
                # Write some test data to the producer FIFO
                if ! echo "test message 1" > "$producer_fifo" 2>/dev/null; then
                    print_error "Failed to write to producer FIFO (message 1)"
                    print_info "FIFO may not have a reader or is in an invalid state"
                    result=1
                fi

                if ! echo "test message 2" > "$producer_fifo" 2>/dev/null; then
                    print_error "Failed to write to producer FIFO (message 2)"
                    print_info "FIFO may not have a reader or is in an invalid state"
                    result=1
                fi

                # Give daemon time to process the data
                sleep 1
            fi
        fi

        # Stop the daemon gracefully
        kill -TERM "$daemon_pid" 2>/dev/null || true

        # Wait for daemon to exit (with timeout)
        local timeout=5
        while [ $timeout -gt 0 ] && kill -0 "$daemon_pid" 2>/dev/null; do
            sleep 1
            timeout=$((timeout - 1))
        done

        # Force kill if still running
        if kill -0 "$daemon_pid" 2>/dev/null; then
            print_error "Daemon did not shut down gracefully within timeout"
            print_info "Force killing daemon"
            kill -9 "$daemon_pid" 2>/dev/null || true
            result=1
        fi
    fi

    # Cleanup
    rm -f "$control_file" "$producer_fifo" "$daemon_log" 2>/dev/null || true

    print_result "FIFO daemon basic functionality" $result
}

# Test: FIFO daemon with batch_size option
test_fifo_daemon_options() {
    echo ""
    echo "Test: FIFO daemon with options"
    echo "--------------------------------"

    local root_path="${TEST_DATA_DIR}/fifo-options"
    mkdir -p "$root_path"

    local control_file="${TEST_DATA_DIR}/fifo-control-options"
    local producer_fifo="${TEST_DATA_DIR}/producer-fifo-options"
    local daemon_log="${TEST_DATA_DIR}/daemon-output-options.log"

    # Create a topic first
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "fifo-options-topic" \
        > /dev/null 2>&1

    # Start the daemon in the background, capturing output
    "$DIASPORA_CTL" fifo \
        --driver "files" \
        --driver.root_path "$root_path" \
        --control-file "$control_file" \
        --logging error \
        > "$daemon_log" 2>&1 &

    local daemon_pid=$!
    local result=0

    # Give the daemon time to start
    sleep 1

    # Check if daemon is running
    if ! kill -0 "$daemon_pid" 2>/dev/null; then
        print_error "Daemon failed to start or crashed immediately"
        print_info "Daemon output:"
        if [ -f "$daemon_log" ]; then
            cat "$daemon_log" | while IFS= read -r line; do
                print_info "  $line"
            done
        else
            print_info "  No log file created"
        fi
        result=1
    else
        # Send a control command with batch_size option
        echo "$producer_fifo -> fifo-options-topic (batch_size=256)" > "$control_file" 2>/dev/null || true

        # Give daemon time to process
        sleep 1

        # Verify producer FIFO was created
        if [ ! -p "$producer_fifo" ]; then
            print_error "Producer FIFO not created with batch_size option"
            print_info "Directory contents: $(ls -la "$(dirname "$producer_fifo")" 2>&1)"
            result=1
        fi

        # Stop the daemon
        kill -TERM "$daemon_pid" 2>/dev/null || true

        # Wait for daemon to exit
        local timeout=5
        while [ $timeout -gt 0 ] && kill -0 "$daemon_pid" 2>/dev/null; do
            sleep 1
            timeout=$((timeout - 1))
        done

        # Force kill if needed
        if kill -0 "$daemon_pid" 2>/dev/null; then
            kill -9 "$daemon_pid" 2>/dev/null || true
            result=1
        fi
    fi

    # Cleanup
    rm -f "$control_file" "$producer_fifo" "$daemon_log" 2>/dev/null || true

    print_result "FIFO daemon with options" $result
}

# Test: FIFO daemon consumer functionality
test_fifo_daemon_consumer() {
    echo ""
    echo "Test: FIFO daemon consumer functionality"
    echo "------------------------------------------"

    local root_path="${TEST_DATA_DIR}/fifo-consumer"
    mkdir -p "$root_path"

    local control_file="${TEST_DATA_DIR}/fifo-control-consumer"
    local producer_fifo="${TEST_DATA_DIR}/producer-fifo-consumer"
    local consumer_fifo="${TEST_DATA_DIR}/consumer-fifo-consumer"
    local daemon_log="${TEST_DATA_DIR}/daemon-output-consumer.log"

    # Create a topic first
    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "consumer-test-topic" \
        > /dev/null 2>&1

    # Start the daemon in the background, capturing output
    "$DIASPORA_CTL" fifo \
        --driver "files" \
        --driver.root_path "$root_path" \
        --control-file "$control_file" \
        --logging debug \
        > "$daemon_log" 2>&1 &

    local daemon_pid=$!
    local result=0

    # Give the daemon time to start
    sleep 1

    # Check if daemon is running
    if ! kill -0 "$daemon_pid" 2>/dev/null; then
        print_error "Daemon failed to start or crashed immediately"
        print_info "Daemon output:"
        if [ -f "$daemon_log" ]; then
            cat "$daemon_log" | while IFS= read -r line; do
                print_info "  $line"
            done
        else
            print_info "  No log file created"
        fi
        result=1
    else
        # PHASE 1: Produce messages to the topic
        print_info "Phase 1: Producing messages to topic"

        # Send producer command
        echo "$producer_fifo -> consumer-test-topic" > "$control_file" 2>/dev/null || true
        sleep 1

        # Verify producer FIFO was created
        if [ ! -p "$producer_fifo" ]; then
            print_error "Producer FIFO not created at $producer_fifo"
            print_info "Directory contents: $(ls -la "$(dirname "$producer_fifo")" 2>&1)"
            result=1
        fi

        if [ $result -eq 0 ]; then
            # Write test data to producer FIFO
            if ! echo "test message 1" > "$producer_fifo" 2>/dev/null; then
                print_error "Failed to write test message 1 to producer FIFO"
                result=1
            fi

            if ! echo "test message 2" > "$producer_fifo" 2>/dev/null; then
                print_error "Failed to write test message 2 to producer FIFO"
                result=1
            fi

            if ! echo "test message 3" > "$producer_fifo" 2>/dev/null; then
                print_error "Failed to write test message 3 to producer FIFO"
                result=1
            fi

            # Give daemon time to process messages into the topic
            sleep 2
            print_info "Phase 1 complete: 3 messages produced to topic"
        fi

        # PHASE 2: Add consumer and read messages from topic
        if [ $result -eq 0 ]; then
            print_info "Phase 2: Adding consumer to read from topic"

            # Create the consumer FIFO (user's responsibility)
            if ! mkfifo "$consumer_fifo" 2>/dev/null; then
                print_error "Failed to create consumer FIFO at $consumer_fifo"
                print_info "Directory contents: $(ls -la "$(dirname "$consumer_fifo")" 2>&1)"
                result=1
            else
                print_info "Created consumer FIFO at $consumer_fifo"
            fi
        fi

        if [ $result -eq 0 ]; then
            # Start reading from consumer FIFO in background BEFORE sending consumer command
            # This ensures a reader is ready when the daemon tries to open the FIFO for writing
            timeout 10s cat "$consumer_fifo" > "${TEST_DATA_DIR}/consumer-output.txt" 2>/dev/null &
            local cat_pid=$!

            # Give cat time to open the FIFO for reading
            sleep 0.5

            # Now send consumer command
            echo "$consumer_fifo <- consumer-test-topic" > "$control_file" 2>/dev/null || true
            sleep 1

            # Give cat time to open the FIFO and consumer to pull/write messages
            sleep 3

            # Stop the cat process
            kill $cat_pid 2>/dev/null || true
            wait $cat_pid 2>/dev/null || true

            # Check if messages were received
            if [ -f "${TEST_DATA_DIR}/consumer-output.txt" ]; then
                local line_count=$(wc -l < "${TEST_DATA_DIR}/consumer-output.txt" 2>/dev/null || echo 0)
                if [ "$line_count" -lt 3 ]; then
                    print_error "Expected at least 3 messages, got $line_count"
                    print_info "Consumer output: $(cat "${TEST_DATA_DIR}/consumer-output.txt" 2>&1)"
                    result=1
                else
                    print_info "Phase 2 complete: $line_count messages consumed from topic"
                fi

                # Verify message content (should contain test messages)
                if ! grep -q "test message 1" "${TEST_DATA_DIR}/consumer-output.txt" 2>/dev/null; then
                    print_error "'test message 1' not found in consumer output"
                    print_info "Consumer output: $(cat "${TEST_DATA_DIR}/consumer-output.txt" 2>&1)"
                    result=1
                fi

                if ! grep -q "test message 2" "${TEST_DATA_DIR}/consumer-output.txt" 2>/dev/null; then
                    print_error "'test message 2' not found in consumer output"
                    print_info "Consumer output: $(cat "${TEST_DATA_DIR}/consumer-output.txt" 2>&1)"
                    result=1
                fi

                if ! grep -q "test message 3" "${TEST_DATA_DIR}/consumer-output.txt" 2>/dev/null; then
                    print_error "'test message 3' not found in consumer output"
                    print_info "Consumer output: $(cat "${TEST_DATA_DIR}/consumer-output.txt" 2>&1)"
                    result=1
                fi
            else
                print_error "Consumer output file not created"
                print_info "cat process may have failed to capture data"
                result=1
            fi
        fi

        # Stop the daemon
        kill -TERM "$daemon_pid" 2>/dev/null || true

        # Wait for daemon to exit
        local timeout=5
        while [ $timeout -gt 0 ] && kill -0 "$daemon_pid" 2>/dev/null; do
            sleep 1
            timeout=$((timeout - 1))
        done

        # Force kill if needed
        if kill -0 "$daemon_pid" 2>/dev/null; then
            kill -9 "$daemon_pid" 2>/dev/null || true
            result=1
        fi
    fi

    # Cleanup
    rm -f "$control_file" "$producer_fifo" "$consumer_fifo" "${TEST_DATA_DIR}/consumer-output.txt" "$daemon_log" 2>/dev/null || true

    print_result "FIFO daemon consumer functionality" $result
}

# Print summary
print_summary() {
    echo ""
    echo "================================================"
    echo "Test Summary"
    echo "================================================"
    echo "Tests run:    $TESTS_RUN"
    echo -e "Tests passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Tests failed: ${RED}$TESTS_FAILED${NC}"
    echo "================================================"

    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
        return 0
    else
        echo -e "${RED}Some tests failed!${NC}"
        return 1
    fi
}

# Main test execution
main() {
    echo "================================================"
    echo "diaspora-ctl Test Suite"
    echo "================================================"

    setup

    # Run all tests
    test_help
    test_create_simple_topic
    test_create_topic_with_partitions
    test_create_topic_with_config_file
    test_component_metadata_files
    test_nested_metadata_parameters
    test_missing_required_args
    test_duplicate_topic
    test_multiple_topics
    test_list_topics_basic
    test_list_topics_verbose
    test_list_topics_empty
    test_fifo_daemon
    test_fifo_daemon_options
    test_fifo_daemon_consumer

    cleanup
    print_summary

    return $?
}

# Run tests
main
exit $?
