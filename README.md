<p align="center">
<img src="img/MofkaLogo-light.svg#gh-light-mode-only" height="220" width="210" />
<img src="img/MofkaLogo-dark.svg#gh-dark-mode-only" height="220" width="210" />
</p>

This repository provides the Mofka API library, an API for streaming frameworks for HPC
applications. The main implementation of this API is [Mofka](https://mofka.readthedocs.io/en/latest/),
which relies on the [Mochi](https://wordpress.cels.anl.gov/mochi/) framework.

Please refer to the [ReadTheDocs](https://mofka.readthedocs.io/) for more information on
how to use Mofka and/or the Mofka API.

Using this API
==============

To implement a streaming service using the Mofka API, you will need to implement classes
inheriting from the following virtual classes:

- `DriverInterface`: represents a connection to your service and provides methods to create and
  open topics. In addition to the pure virtual functions to implement, your class must provide a
  `static std::shared_ptr<DriverInterface> create(const Metadata&)` factory method. Additionally,
  your library must call `MOFKA_REGISTER_DRIVER(you_backend_name, YourBackendType)` in one of its
  .cpp files.
- `ThreadPoolInterface`: represents a set of threads to which work can be submitted.
- `TopicHandleInterface`: represents a topic managed by your service, and provides functions
  to instantiate producers and consumers for this topic.
- `ProducerInterface`: represents a producer producing events to a given topic.
- `ConsumerInterface`: represents a consumer consuming events from a given topic.
- `EventInterface`: represents an event from your service. It must implement an `acknowledge` method
  as well as methods to access the underlying metadata and data.

Each of these classes has a non-interface counterpart (Driver, ThreadPool, TopicHandle, Producer,
Consumer, and Event). These classes are simple wrappers around a `shared_ptr` to the above
interfaces, and will be what the user of your service uses.

The [tests/SimpleBackend.hpp](tests/SimpleBackend.hpp) is an example of implementation of the
Mofka API, implementing single-partition queues that are local to a process.
