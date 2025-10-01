#include <iostream>
#include <string>
#include <tclap/CmdLine.h>

int main(int argc, char** argv)
{
    try
    {
        TCLAP::CmdLine cmd("Diaspora Producer Benchmark", ' ', "0.1");

        TCLAP::ValueArg<std::string> driverArg(
            "d", "driver", "Driver to use", true, "kafka", "string");
        TCLAP::ValueArg<std::string> configFileArg(
            "c", "driver-config", "Configuration file for the driver", false, "", "string");
        TCLAP::ValueArg<std::string> topicArg(
            "t", "topic", "Topic to produce to", true, "test-topic", "string");
        TCLAP::ValueArg<size_t> numEventsArg(
            "n", "num-events", "Number of events to send", false, 1000, "int");
        TCLAP::ValueArg<size_t> dataSizeArg(
            "s", "data-size", "Size of the data in each event in bytes", false, 0, "int");
        TCLAP::ValueArg<size_t> metadataSizeArg(
            "m", "metadata-size", "Size of the metadata part of each event in bytes", false, 0, "int");
        TCLAP::ValueArg<size_t> batchSizeArg(
            "b", "batch-size", "Number of events to batch together", false, 1, "int");
        TCLAP::ValueArg<size_t> numThreadsArg(
            "p", "num-threads", "Number of background threads for the producer to use", false, 1, "int");
        TCLAP::ValueArg<size_t> flushIntervalArg(
            "f", "flush-interval", "Number of events to push before calling flush", false, 1000, "int");

        cmd.add(driverArg);
        cmd.add(configFileArg);
        cmd.add(topicArg);
        cmd.add(numEventsArg);
        cmd.add(dataSizeArg);
        cmd.add(metadataSizeArg);
        cmd.add(batchSizeArg);
        cmd.add(numThreadsArg);
        cmd.add(flushIntervalArg);

        cmd.parse(argc, argv);

        std::string driver     = driverArg.getValue();
        std::string configFile = configFileArg.getValue();
        std::string topic      = topicArg.getValue();
        auto numEvents         = numEventsArg.getValue();
        auto dataSize          = dataSizeArg.getValue();
        auto metadataSize      = metadataSizeArg.getValue();
        auto batchSize         = batchSizeArg.getValue();
        auto numThreads        = numThreadsArg.getValue();
        auto flushInterval     = flushIntervalArg.getValue();

        std::cout << "Driver: " << driver << std::endl;
        std::cout << "Driver config file: " << configFile << std::endl;
        std::cout << "Topic: " << topic << std::endl;
        std::cout << "Number of events: " << numEvents << std::endl;
        std::cout << "Data size: " << dataSize << std::endl;
        std::cout << "Metadata size: " << metadataSize << std::endl;
        std::cout << "Batch size: " << batchSize << std::endl;
        std::cout << "Number of threads: " << numThreads << std::endl;
        std::cout << "Flush interval: " << flushInterval << std::endl;

    }
    catch (TCLAP::ArgException &e)
    {
        std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
    }

    return 0;
}
