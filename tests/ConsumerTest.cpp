/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include "SimpleBackend.hpp"

DIASPORA_REGISTER_DRIVER(_, simple, SimpleDriver);

TEST_CASE("Consumer test", "[consumer]") {

    SECTION("Initialize a Driver and create/open a topic") {
        diaspora::Metadata options;
        diaspora::Driver driver = diaspora::Driver::New("simple", options);
        REQUIRE(static_cast<bool>(driver));
        diaspora::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        REQUIRE_NOTHROW(driver.createTopic("mytopic"));
        REQUIRE_THROWS_AS(driver.createTopic("mytopic"), diaspora::Exception);

        REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));
        REQUIRE_THROWS_AS(driver.openTopic("mytopic2"), diaspora::Exception);

        SECTION("Create a consumer from the topic") {
            diaspora::Consumer consumer;
            REQUIRE(!static_cast<bool>(consumer));
            REQUIRE_NOTHROW(consumer = topic.consumer("myconsumer"));
            REQUIRE(static_cast<bool>(consumer));
            REQUIRE(consumer.name() == "myconsumer");
            REQUIRE(static_cast<bool>(consumer.topic()));
            REQUIRE(consumer.topic().name() == "mytopic");
        }
    }
}
