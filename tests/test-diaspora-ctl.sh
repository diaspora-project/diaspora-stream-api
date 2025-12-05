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

# Test: Display help
test_help() {
    echo ""
    echo "Test: Display help"
    echo "-------------------"

    "$DIASPORA_CTL" --help > /dev/null 2>&1
    print_result "diaspora-ctl --help" $?
}

# Test: Create a simple topic
test_create_simple_topic() {
    echo ""
    echo "Test: Create a simple topic"
    echo "----------------------------"

    local root_path="${TEST_DATA_DIR}/simple"
    mkdir -p "$root_path"

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "test-topic-1" \
        > /dev/null 2>&1

    local result=$?

    # Verify topic directory was created
    if [ -d "$root_path/test-topic-1" ]; then
        result=0
    else
        result=1
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

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "multi-partition-topic" \
        --topic.num_partitions 4 \
        > /dev/null 2>&1

    local result=$?

    # Verify partition directories were created (00000000, 00000001, 00000002, 00000003)
    if [ -d "$root_path/multi-partition-topic/partitions/00000000" ] && \
       [ -d "$root_path/multi-partition-topic/partitions/00000001" ] && \
       [ -d "$root_path/multi-partition-topic/partitions/00000002" ] && \
       [ -d "$root_path/multi-partition-topic/partitions/00000003" ]; then
        result=0
    else
        result=1
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

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver-config "$driver_config" \
        --name "config-topic" \
        --topic-config "$topic_config" \
        > /dev/null 2>&1

    local result=$?

    # Verify topic was created with 2 partitions
    if [ -d "$root_path/config-topic/partitions/00000000" ] && \
       [ -d "$root_path/config-topic/partitions/00000001" ]; then
        result=0
    else
        result=1
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

    "$DIASPORA_CTL" topic create \
        --driver "files" \
        --driver.root_path "$root_path" \
        --name "component-topic" \
        > /dev/null 2>&1

    local result=0

    # Verify component JSON files exist
    if [ ! -f "$root_path/component-topic/validator.json" ]; then
        echo "  validator.json not found"
        result=1
    fi

    if [ ! -f "$root_path/component-topic/serializer.json" ]; then
        echo "  serializer.json not found"
        result=1
    fi

    if [ ! -f "$root_path/component-topic/partition-selector.json" ]; then
        echo "  partition-selector.json not found"
        result=1
    fi

    # Verify JSON files are valid JSON
    if [ $result -eq 0 ]; then
        python3 -c "import json; json.load(open('$root_path/component-topic/validator.json'))" 2>/dev/null || result=1
        python3 -c "import json; json.load(open('$root_path/component-topic/serializer.json'))" 2>/dev/null || result=1
        python3 -c "import json; json.load(open('$root_path/component-topic/partition-selector.json'))" 2>/dev/null || result=1
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

    cleanup
    print_summary

    return $?
}

# Run tests
main
exit $?
