/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <cstdlib>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include "SimpleBackend.hpp"

DIASPORA_REGISTER_DRIVER(_, simple, SimpleDriver);

TEST_CASE("Driver test", "[driver]") {

    const char* backend      = std::getenv("DIASPORA_TEST_BACKEND");
    const char* backend_args = std::getenv("DIASPORA_TEST_BACKEND_ARGS");
    const char* topic_args   = std::getenv("DIASPORA_TEST_TOPIC_ARGS");
    backend                  = backend ? backend : "simple";
    backend_args             = backend_args ? backend_args : "{}";
    topic_args               = topic_args ? topic_args : "{}";

    SECTION("Initialize a service handle") {
        diaspora::Driver driver;
        diaspora::Metadata options{backend_args};
        driver = diaspora::Driver::New(backend, options);
        REQUIRE(static_cast<bool>(driver));

        SECTION("Create a topic") {
            diaspora::TopicHandle topic;
            REQUIRE(!static_cast<bool>(topic));
            REQUIRE(!driver.topicExists("mytopic"));
            REQUIRE_NOTHROW(driver.createTopic("mytopic", diaspora::Metadata{topic_args}));
            REQUIRE(driver.topicExists("mytopic"));
            REQUIRE_THROWS_AS(driver.createTopic("mytopic", diaspora::Metadata{topic_args}),
                              diaspora::Exception);

            REQUIRE_NOTHROW(topic = driver.openTopic("mytopic"));
            REQUIRE(static_cast<bool>(topic));
            REQUIRE_THROWS_AS(driver.openTopic("mytopic2"), diaspora::Exception);

            REQUIRE(topic.partitions().size() == 1);
        }
    }
}
