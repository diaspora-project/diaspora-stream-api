#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <fstream>
#include <stdexcept>
#include <tclap/CmdLine.h>
#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include <diaspora/Consumer.hpp>
#include <diaspora/Exception.hpp>
#include <mpi.h>

struct Options
{
    std::string driver;
    std::string configFile;
    std::string topic;
    size_t numEvents;
    size_t numThreads;
};

void parse_options(int argc, char** argv, Options& options);
void print_options(const Options& options);
void run_benchmark(const Options& options);

int main(int argc, char** argv)
{
    MPI_Init(&argc, &argv);
    int ret = 0;
    try
    {
        Options options;
        parse_options(argc, argv, options);
        int rank;
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        if (rank == 0) {
            print_options(options);
        }
        run_benchmark(options);
    }
    catch (const TCLAP::ArgException &e)
    {
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
        ret = -1;
    }
    catch (const diaspora::Exception& e)
    {
        std::cerr << "diaspora error: " << e.what() << std::endl;
        ret = -1;
    }
    catch (const std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
        ret = -1;
    }
    MPI_Finalize();
    return ret;
}

void run_benchmark(const Options& options)
{
    // Read driver configuration from file if provided
    diaspora::Metadata driver_opts;
    if (!options.configFile.empty()) {
        std::ifstream configFileStream(options.configFile);
        if (!configFileStream) {
            throw std::runtime_error("Could not open config file: " + options.configFile);
        }
        std::string configContent((std::istreambuf_iterator<char>(configFileStream)),
                                    std::istreambuf_iterator<char>());
        driver_opts = diaspora::Metadata{configContent};
    }

    // Initialize Driver
    auto driver = diaspora::Driver::New(options.driver.c_str(), driver_opts);

    // Create ThreadPool
    auto threadPool = driver.makeThreadPool(diaspora::ThreadCount{options.numThreads});

    // Open Topic
    auto topic = driver.openTopic(options.topic);

    // Create Consumer
    auto consumer = topic.consumer("benchmark-consumer", threadPool);

    // Run benchmark
    MPI_Barrier(MPI_COMM_WORLD);
    auto start_time = std::chrono::high_resolution_clock::now();

    size_t events_received = 0;
    while(events_received < options.numEvents) {
        auto event = consumer.pull().wait();
        events_received += 1;
    }

    MPI_Barrier(MPI_COMM_WORLD);
    auto end_time = std::chrono::high_resolution_clock::now();

    // Calculate and print results
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        std::chrono::duration<double> elapsed = end_time - start_time;
        double total_time_sec = elapsed.count();
        double events_per_sec = options.numEvents / total_time_sec;

        std::cout << "\n--- Benchmark Results ---" << std::endl;
        std::cout << "Total time: " << total_time_sec << " s" << std::endl;
        std::cout << "Total events: " << options.numEvents << " s" << std::endl;
        std::cout << "Throughput: " << events_per_sec << " events/s" << std::endl;
    }
}

void parse_options(int argc, char** argv, Options& options)
{
    TCLAP::CmdLine cmd("Diaspora Consumer Benchmark", ' ', "0.1");

    TCLAP::ValueArg<std::string> driverArg(
        "d", "driver", "Driver to use", true, "kafka", "string");
    TCLAP::ValueArg<std::string> configFileArg(
        "c", "driver-config", "Configuration file for the driver", false, "", "string");
    TCLAP::ValueArg<std::string> topicArg(
        "t", "topic", "Topic to consume from", true, "test-topic", "string");
    TCLAP::ValueArg<size_t> numEventsArg(
        "n", "num-events", "Number of events to receive", false, 1000, "int");
    TCLAP::ValueArg<size_t> numThreadsArg(
        "p", "num-threads", "Number of background threads for the consumer to use", false, 1, "int");

    cmd.add(driverArg);
    cmd.add(configFileArg);
    cmd.add(topicArg);
    cmd.add(numEventsArg);
    cmd.add(numThreadsArg);

    cmd.parse(argc, argv);

    options.driver = driverArg.getValue();
    options.configFile = configFileArg.getValue();
    options.topic = topicArg.getValue();
    options.numEvents = numEventsArg.getValue();
    options.numThreads = numThreadsArg.getValue();
}

void print_options(const Options& options)
{
    std::cout << "--- Benchmark Options ---" << std::endl;
    std::cout << "Driver: " << options.driver << std::endl;
    std::cout << "Driver config file: " << options.configFile << std::endl;
    std::cout << "Topic: " << options.topic << std::endl;
    std::cout << "Number of events: " << options.numEvents << std::endl;
    std::cout << "Number of threads: " << options.numThreads << std::endl;
}
