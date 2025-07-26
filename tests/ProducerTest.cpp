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

TEST_CASE("Producer test", "[producer]") {

    SECTION("Initialize a Driver and create/open a topic") {

        diaspora::Metadata options;
        diaspora::Driver driver = diaspora::Driver::New("simple", options);
        REQUIRE(static_cast<bool>(driver));

        diaspora::TopicHandle topic;
        REQUIRE(!static_cast<bool>(topic));
        REQUIRE_NOTHROW(driver.createTopic("mytopic"));
        REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
        REQUIRE(static_cast<bool>(topic));

        SECTION("Create a producer from the topic") {
            diaspora::Producer producer;
            REQUIRE(!static_cast<bool>(producer));
            producer = topic.producer("myproducer");
            REQUIRE(static_cast<bool>(producer));
            REQUIRE(producer.name() == "myproducer");
            REQUIRE(static_cast<bool>(producer.topic()));
            REQUIRE(producer.topic().name() == "mytopic");
        }
    }
}
