/*
 * (C) 2025 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_all.hpp>
#include <mofka/Metadata.hpp>

TEST_CASE("Mofka API Metadata test", "[metadata]") {

    SECTION("Default Metadata object") {

        auto md = mofka::Metadata{};
        REQUIRE(static_cast<bool>(md));
        REQUIRE(md.isValidJson());
        REQUIRE(md.json().is_object());
        REQUIRE(md.string() == "{}");

        SECTION("Modify metadata using string accessor") {
            md.string() = R"({"x":1,"y":2.3})";
            REQUIRE(static_cast<bool>(md));
            REQUIRE(md.isValidJson());
            REQUIRE(md.json().is_object());
            REQUIRE(md.json().contains("x"));
            REQUIRE(md.json().contains("y"));
            REQUIRE(md.string() == R"({"x":1,"y":2.3})");
        }

        SECTION("Modify metadata using json accessor") {
            md.json()["x"] = 1;
            md.json()["y"] = 2.3;
            REQUIRE(static_cast<bool>(md));
            REQUIRE(md.isValidJson());
            REQUIRE(md.json().is_object());
            REQUIRE(md.json().contains("x"));
            REQUIRE(md.json().contains("y"));
            REQUIRE(md.string() == R"({"x":1,"y":2.3})");
        }
    }

    SECTION("Constructors without validate") {

        SECTION("string constructor") {
            std::string content{R"({"x":1,"y":2.3})"};
            auto md = mofka::Metadata{content, false};
            REQUIRE(md.isValidJson());
            REQUIRE(md.json().is_object());
            REQUIRE(md.json().contains("x"));
            REQUIRE(md.json().contains("y"));
            REQUIRE(md.string() == R"({"x":1,"y":2.3})");
        }

        SECTION("string_view constructor") {
            std::string_view content{R"({"x":1,"y":2.3})"};
            auto md = mofka::Metadata{content, false};
            REQUIRE(md.isValidJson());
            REQUIRE(md.json().is_object());
            REQUIRE(md.json().contains("x"));
            REQUIRE(md.json().contains("y"));
            REQUIRE(md.string() == R"({"x":1,"y":2.3})");
        }

        SECTION("const char* constructor") {
            const char* content = R"({"x":1,"y":2.3})";
            auto md = mofka::Metadata{content, false};
            REQUIRE(md.isValidJson());
            REQUIRE(md.json().is_object());
            REQUIRE(md.json().contains("x"));
            REQUIRE(md.json().contains("y"));
            REQUIRE(md.string() == R"({"x":1,"y":2.3})");
        }

        SECTION("string constructor (invalid JSON)") {
            std::string content{R"({"x":1,"y":)"};
            auto md = mofka::Metadata{content, false};
            REQUIRE(!md.isValidJson());
            REQUIRE_THROWS_AS(md.json(), mofka::Exception);
            REQUIRE(md.string() == R"({"x":1,"y":)");
        }

        SECTION("string_view constructor (invalid JSON)") {
            std::string_view content{R"({"x":1,"y":)"};
            auto md = mofka::Metadata{content, false};
            REQUIRE(!md.isValidJson());
            REQUIRE_THROWS_AS(md.json(), mofka::Exception);
            REQUIRE(md.string() == R"({"x":1,"y":)");
        }

        SECTION("const char* constructor (invalid JSON)") {
            const char* content = R"({"x":1,"y":)";
            auto md = mofka::Metadata{content, false};
            REQUIRE(!md.isValidJson());
            REQUIRE_THROWS_AS(md.json(), mofka::Exception);
            REQUIRE(md.string() == R"({"x":1,"y":)");
        }

    }

    SECTION("Constructors with validate") {

        SECTION("string constructor") {
            std::string content{R"({"x":1,"y":2.3})"};
            auto md = mofka::Metadata{content, true};
            REQUIRE(md.isValidJson());
            REQUIRE(md.json().is_object());
            REQUIRE(md.json().contains("x"));
            REQUIRE(md.json().contains("y"));
            REQUIRE(md.string() == R"({"x":1,"y":2.3})");
        }

        SECTION("string_view constructor") {
            std::string_view content{R"({"x":1,"y":2.3})"};
            auto md = mofka::Metadata{content, true};
            REQUIRE(md.isValidJson());
            REQUIRE(md.json().is_object());
            REQUIRE(md.json().contains("x"));
            REQUIRE(md.json().contains("y"));
            REQUIRE(md.string() == R"({"x":1,"y":2.3})");
        }

        SECTION("const char* constructor") {
            const char* content = R"({"x":1,"y":2.3})";
            auto md = mofka::Metadata{content, true};
            REQUIRE(md.isValidJson());
            REQUIRE(md.json().is_object());
            REQUIRE(md.json().contains("x"));
            REQUIRE(md.json().contains("y"));
            REQUIRE(md.string() == R"({"x":1,"y":2.3})");
        }

        SECTION("string constructor (invalid JSON)") {
            std::string content{R"({"x":1,"y":)"};
            REQUIRE_THROWS_AS(mofka::Metadata(content, true), mofka::Exception);
        }

        SECTION("string_view constructor (invalid JSON)") {
            std::string_view content{R"({"x":1,"y":)"};
            REQUIRE_THROWS_AS(mofka::Metadata(content, true), mofka::Exception);
        }

        SECTION("const char* constructor (invalid JSON)") {
            const char* content = R"({"x":1,"y":)";
            REQUIRE_THROWS_AS(mofka::Metadata(content, true), mofka::Exception);
        }

    }

}
