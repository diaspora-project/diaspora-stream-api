/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include "SimpleBackend.hpp"

DIASPORA_REGISTER_DRIVER(_, simple, SimpleDriver);

TEST_CASE("Diaspora API test", "[diaspora-api]") {

    SECTION("Create invalid Driver") {

        REQUIRE_THROWS_AS(
            diaspora::DriverFactory::create("unknown", {}),
            diaspora::Exception);

        REQUIRE_THROWS_AS(
            diaspora::DriverFactory::create("unknown:libunknown.so", {}),
            diaspora::Exception);

    }

    SECTION("Create Driver") {

        auto driver = diaspora::DriverFactory::create("simple", {});
        REQUIRE(static_cast<bool>(driver));

    }
}
