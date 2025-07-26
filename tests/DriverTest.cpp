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

TEST_CASE("Driver test", "[driver]") {

    SECTION("Initialize a service handle") {
        diaspora::Driver driver;
        diaspora::Metadata options;
        driver = diaspora::Driver::New("simple", options);
        REQUIRE(static_cast<bool>(driver));

        SECTION("Create a topic") {
            diaspora::TopicHandle topic;
            REQUIRE(!static_cast<bool>(topic));
            REQUIRE(!driver.topicExists("mytopic"));
            REQUIRE_NOTHROW(driver.createTopic("mytopic"));
            REQUIRE(driver.topicExists("mytopic"));
            REQUIRE_THROWS_AS(driver.createTopic("mytopic"), diaspora::Exception);

            REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
            REQUIRE(static_cast<bool>(topic));
            REQUIRE_THROWS_AS(driver.openTopic("mytopic2"), diaspora::Exception);

            REQUIRE(topic.partitions().size() == 1);

            //std::cerr << "---------------------------------------------" << std::endl;
            //std::cerr << server.getCurrentConfig() << std::endl;
        }
    }
}
