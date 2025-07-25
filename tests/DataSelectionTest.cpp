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

TEST_CASE("Event consumer test", "[event-consumer]") {

    diaspora::Metadata options;
    diaspora::Driver driver = diaspora::Driver::New("simple", options);
    REQUIRE(static_cast<bool>(driver));

    driver.createTopic("mytopic");
    auto topic = driver.openTopic("mytopic");

    std::string seg1 = "abcdefghijklmnopqrstuvwxyz";
    std::string seg2 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    // Producer
    {
        auto producer = topic.producer();
        REQUIRE(static_cast<bool>(producer));
        auto metadata = diaspora::Metadata{};
        auto data = diaspora::DataView{{{seg1.data(), seg1.size()},{seg2.data(), seg2.size()}}};
        producer.push(metadata, data);
        producer.flush();
    }
    topic.markAsComplete();

    SECTION("Consume no data") {
        diaspora::DataSelector data_selector =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor&) {
                return diaspora::DataDescriptor();
            };
        diaspora::DataAllocator data_allocator =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor&) {
                return diaspora::DataView{};
            };
        auto consumer = topic.consumer(
                "myconsumer", data_selector, data_allocator);
        auto event = consumer.pull().wait();
        REQUIRE(event.data().size() == 0);
        REQUIRE(consumer.pull().wait().id() == diaspora::NoMoreEvents);
    }

    SECTION("Consume the whole data") {
        diaspora::DataSelector data_selector =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                return descriptor;
            };
        diaspora::DataAllocator data_broker =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto data = new char[size];
                return diaspora::DataView{data, size};
            };
        auto consumer = topic.consumer(
                "myconsumer", data_selector, data_broker);
        auto event = consumer.pull().wait();
        REQUIRE(event.data().size() == 52);
        REQUIRE(event.data().segments().size() == 1);
        auto received = std::string_view{
            (const char*)event.data().segments()[0].ptr,
            event.data().segments()[0].size};
        REQUIRE(received == seg1+seg2);
        delete[] (char*)event.data().segments()[0].ptr;
        REQUIRE(consumer.pull().wait().id() == diaspora::NoMoreEvents);
    }

    SECTION("Consume using makeSubView") {
        diaspora::DataSelector data_selector =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                return descriptor.makeSubView(13, 26);
            };
        diaspora::DataAllocator data_broker =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto data = new char[size];
                return diaspora::DataView{data, size};
            };
        auto consumer = topic.consumer(
                "myconsumer", data_selector, data_broker);
        auto event = consumer.pull().wait();
        REQUIRE(event.data().size() == 26);
        REQUIRE(event.data().segments().size() == 1);
        auto received = std::string_view{
            (const char*)event.data().segments()[0].ptr,
            event.data().segments()[0].size};
        REQUIRE(received == "nopqrstuvwxyzABCDEFGHIJKLM");
        delete[] (char*)event.data().segments()[0].ptr;
        REQUIRE(consumer.pull().wait().id() == diaspora::NoMoreEvents);
    }

    SECTION("Consume using makeStridedView") {
        diaspora::DataSelector data_selector =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                return descriptor.makeStridedView(13, 3, 4, 2);
            };
        diaspora::DataAllocator data_allocator =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto data = new char[size];
                return diaspora::DataView{data, size};
            };
        auto consumer = topic.consumer(
                "myconsumer", data_selector, data_allocator);
        auto event = consumer.pull().wait();
        REQUIRE(event.data().size() == 12);
        REQUIRE(event.data().segments().size() == 1);
        auto received = std::string_view{
            (const char*)event.data().segments()[0].ptr,
            event.data().segments()[0].size};
        REQUIRE(received == "nopqtuvwzABC");
        delete[] (char*)event.data().segments()[0].ptr;
        REQUIRE(consumer.pull().wait().id() == diaspora::NoMoreEvents);
    }

    SECTION("Consume using makeUnstructuredView") {
        diaspora::DataSelector data_selector =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                return descriptor.makeUnstructuredView({
                        {3, 6},
                        {15, 4},
                        {27, 8}
                });
            };
        diaspora::DataAllocator data_allocator =
            [](const diaspora::Metadata&, const diaspora::DataDescriptor& descriptor) {
                auto size = descriptor.size();
                auto data = new char[size];
                return diaspora::DataView{data, size};
            };
        auto consumer = topic.consumer(
                "myconsumer", data_selector, data_allocator);
        auto event = consumer.pull().wait();
        REQUIRE(event.data().size() == 18);
        REQUIRE(event.data().segments().size() == 1);
        auto received = std::string_view{
            (const char*)event.data().segments()[0].ptr,
            event.data().segments()[0].size};
        REQUIRE(received == "defghipqrsBCDEFGHI");
        delete[] (char*)event.data().segments()[0].ptr;
        REQUIRE(consumer.pull().wait().id() == diaspora::NoMoreEvents);
    }
}
