/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include "SimpleBackend.hpp"

MOFKA_REGISTER_DRIVER(simple, SimpleDriver);

TEST_CASE("Mofka API test", "[mofka-api]") {

    SECTION("Create invalid Driver") {

        REQUIRE_THROWS_AS(
            mofka::DriverFactory::create("unknown", {}),
            mofka::Exception);

    }

    SECTION("Create Driver") {

        auto driver = mofka::DriverFactory::create("simple", {});
        REQUIRE(static_cast<bool>(driver));

    }
}
