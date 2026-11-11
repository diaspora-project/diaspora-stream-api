Utilities
=========

The Diaspora Stream API comes with the `diaspora-ctl` executable, which can be used
for a number of things listed in this section.

Default options via environment variable
----------------------------------------

To avoid retyping the same driver options on every invocation, set the
:code:`DIASPORA_CTL_DRIVER_OPTIONS` environment variable. Its contents are
tokenized using shell-like quoting (single and double quotes, backslash
escapes; **no** variable expansion or command substitution) and prepended to
the command-line arguments before parsing. Any option provided on the actual
command line takes precedence over the same option supplied via the
environment variable.

.. code-block:: bash

   export DIASPORA_CTL_DRIVER_OPTIONS="--driver files --driver.path /tmp/my-stream"

   # Equivalent to:
   #   diaspora-ctl topic list --driver files --driver.path /tmp/my-stream
   diaspora-ctl topic list

   # CLI overrides the env-var value of --driver.path
   diaspora-ctl topic list --driver.path /tmp/other-stream

This affects the :code:`topic create`, :code:`topic list`, and :code:`fifo`
subcommands. The :code:`forward` subcommand is configured exclusively through
its TOML file and ignores this variable.

.. note::

   Defining the variable as a plain shell string and expanding it inline (e.g.
   :code:`diaspora-ctl topic list $MY_OPTS`) is fragile across shells: in zsh,
   unquoted parameter expansion does not perform word-splitting, so the whole
   string is passed as a single argument. Setting
   :code:`DIASPORA_CTL_DRIVER_OPTIONS` lets :code:`diaspora-ctl` itself do the
   tokenization, which works identically in any shell.

Manipulating topics
-------------------

diaspora-ctl topic create
^^^^^^^^^^^^^^^^^^^^^^^^^

This command allows creating a new topic, and can be used as follows.

.. code-block:: bash

   diaspora-ctl topic create --name <topic-name> --driver <driver-name>

Or more extensively:

.. code-block:: bash

   diaspora-ctl topic create --name <topic-name> \
        --driver <driver-name> \
        --driver.<option> <value> ... \
        --driver-config <driver-config.json> \
        --topic.<option> <value> ... \
        --topic-config <topic-config.json> \
        --validator <validator-name> \
        --validator-config <validator-config.json> \
        --validator.<option> <value> ... \
        --serializer <serializer-name> \
        --serializer.<option> <value> ... \
        --serializer-config <serializer-config.json> \
        --partition-selector <partition-selector-name> \
        --partition-selector-config <partition-selector-config.json> \
        --partition-selector.<option> <value>

The only mandatory arguments are the :code:`--name` and :code:`--driver`. Options
can then be provided to each component using either :code:`--<component>-config <file.json>`
or a set of :code:`--<component>.<option> <value>` parameters.

diaspora-ctl topic list
^^^^^^^^^^^^^^^^^^^^^^^

This command lists the available topics. It is used as follows.


.. code-block:: bash

   diaspora-ctl topic list --driver <driver-name>

This will print each topic name on its own line on the standard output.
If :code:`--verbose` is provided, a JSON-formatted information about the topic is added to
each line.

Creating a FIFO daemon
----------------------

In some applications, it can be convenient for clients to interact with the
streaming engine via a file descriptor rather than linking against the Diaspora
Stream API library and using the API. The :code:`diaspora-ctl fifo` command
is here to help with that.

diaspora-ctl fifo
^^^^^^^^^^^^^^^^^

This command is used as follows.

.. code-block:: bash

   diaspora-ctl fifo --driver <driver-name> \
                     --driver.<option> <value> \
                     --driver-config <driver-config.json> \
                     --control-file <control-file>

The only mandatory arguments are the :code:`--driver` and :code:`--control-file`, as well
as any option the driver may require.

This command blocks until killed, so it is best used as a daemon put in the background.
Upon starting, the control file will be created. This file allows sending commands to
to the daemon. These commands can be of two types, producer and consumer commands,
shown hereafter.

.. code-block:: bash

   # Producer command
   echo 'path -> topic (key1=value1, key2=value2, ...)' > control-file

   # Consumer command
   echo 'path <- topic (key1=value1, key2=value2, ...)' > control-file

The only difference is the direction of the arrow.

Producer command
""""""""""""""""

A producer command will make the daemon create a FIFO with the specified `path`
and a producer instance linked to the specified `topic`. Any line of text written into
this FIFO will be passed to the producer as metadata.

The FIFO is persistent: it is created once and stays in place until the daemon shuts
down. A writer may open it, write one or more newline-delimited events, and close it,
then reopen the same path later to send more events; there is no need to send the
control command again. Each such open/write/close cycle flushes that cycle's events to
storage when the writer closes. A writer may also hold the FIFO open and stream events
continuously. In both cases the daemon reads the whole FIFO before acting on the writer
closing, so events are never dropped, even when a single write is larger than the
operating-system pipe buffer. Batching still governs when full batches reach storage
(see :code:`batch_size` below); closing the FIFO flushes any remaining buffered events.

Options in parenthesis may be one of the following.

* :code:`format` : :code:`raw` or :code:`json` (default: :code:`raw`). If :code:`json` is
  specified, the line is interpreted as a JSON document. Otherwise it is interpreted as a
  string. Note that because the daemon splits events on new lines, these JSON or strings
  cannot themselves embed new lines.

* :code:`batch_size` : an integer value, representing the batch size the producer uses
  (default is 128).

Note that only metadata can be written into the topic, the data part of events is always left empty.

Batching and when events become visible
+++++++++++++++++++++++++++++++++++++++

The producer groups events into batches of :code:`batch_size` events. A batch reaches
storage (and so becomes visible to consumers) only when one of two things happens:

* the batch fills up (``batch_size`` events have been pushed), or
* the writer closes the FIFO, which flushes the current, possibly partial, batch.

The daemon detects a writer closing the FIFO (a *hang-up*) and flushes on every such
event. This makes the *process model of the writer* the deciding factor for how events
are grouped, which is easy to overlook because two shell idioms that look similar behave
very differently.

**One writer per event (a loop of redirections).** Each :code:`echo ... > fifo` opens the
FIFO, writes one line, and closes it. The daemon sees a hang-up after every line and
flushes, so events reach storage one by one, **regardless of** :code:`batch_size`.

.. code-block:: bash

   # A new writer process per iteration: each redirection opens and closes the FIFO.
   echo 'prod.fifo -> my-topic (batch_size=128)' > control-file
   for i in $(seq 1 10); do
       echo "event-$i" > prod.fifo   # opens, writes one line, closes -> flush
       sleep 1
   done

In this case, even though :code:`batch_size` is 128, a consumer sees one new event every
second, because each closing of the FIFO flushes that single event.

**One long-lived writer (the whole loop is piped).** When a single process holds the FIFO
open and streams into it, there is no hang-up between events. The producer then fills
batches normally: nothing is stored until the batch reaches :code:`batch_size` events (or
the writer finally closes the FIFO).

.. code-block:: bash

   # A single writer holds the FIFO open for the whole loop.
   echo 'prod.fifo -> my-topic (batch_size=128)' > control-file

   emit_events() {
       for i in $(seq 1 1000); do
           echo "event-$i"
           sleep 1
       done
   }
   emit_events > prod.fifo   # one process, one open/close, held open for the loop

Here the loop still produces one event per second, but the FIFO stays open, so no batch is
flushed until 128 events have accumulated. With the default :code:`batch_size` of 128 and a
one-second interval, a consumer has to wait about **128 seconds** before the first batch of
128 events is stored and becomes available; the next batch appears 128 seconds later, and
so on. The partial final batch is flushed only when :code:`emit_events` returns and the
FIFO is closed.

To make a long-lived writer deliver events promptly, lower the batch size. Setting
:code:`batch_size=1` flushes after every event, so events become visible as soon as they
are produced, at the cost of one storage flush per event.

.. code-block:: bash

   # Long-lived writer, but every event is flushed immediately.
   echo 'prod.fifo -> my-topic (batch_size=1)' > control-file
   emit_events > prod.fifo

Consumer command
""""""""""""""""

A consumer command works in a similar manner, however `the specified FIFO device must have been
created first and a process must have opened it in read mode`. The daemon will create a consumer
linked to the topic and write any received metadata into the FIFO device.

The required order is therefore:

.. code-block:: bash

   mkfifo cons.fifo                            # 1. create the FIFO
   cat cons.fifo &                             # 2. open it in read mode (a reader must be present)
   echo 'cons.fifo <- my-topic' > control-file # 3. register the consumer with the daemon

A reader must be present shortly after the command is sent: the daemon opens the FIFO for writing
in non-blocking mode and retries for a few seconds, then gives up if no reader has appeared, and
the consumer does not start delivering. Opening the reader before sending the control command (as
above) avoids this race.

Options may again be provided in parenthesis, with currently supported options as follows.

* :code:`batch_size` : an integer value, used as a read/prefetch hint for the consumer
  (default is 128). Unlike on the producer side, this does **not** delay delivery: the daemon
  writes each event to the FIFO as soon as it is read from storage, so the consumer sees events
  at the rate they become available regardless of :code:`batch_size`. A larger value can
  improve throughput when a backlog is being drained by reading events in larger chunks.

Behavior and delivery guarantees
++++++++++++++++++++++++++++++++

* **The consumer streams events one per line**, in the order they were stored, writing each
  event's metadata followed by a newline. With the default :code:`raw` format a string event is
  unquoted before being written; with :code:`format=json` the raw JSON is written as stored.

* **The consumer live-tails the topic.** It first drains whatever is already stored, then keeps
  delivering new events as producers append them. It does not stop when it catches up with the
  producer; it blocks, waiting for the next event, until the daemon is shut down or the reader
  closes the FIFO. A consumer may therefore be registered before any event exists and will still
  receive events produced afterwards.

* **Delivery depends on events being visible in storage.** A consumer can only deliver events
  that have actually reached storage, so producer batching directly affects what a consumer sees.
  If a long-lived producer is buffering a large batch (see the producer section above), those
  events are not yet stored and the consumer will not receive them until the batch is flushed.
  When prompt end-to-end delivery is needed, use a producer configuration that flushes often (for
  example :code:`batch_size=1`, or a one-writer-per-event loop).

* **Back-pressure is respected.** If the reading process is slower than the stream and the FIFO
  fills up, the daemon waits and retries rather than dropping events.

Forwarding events between topics
---------------------------------

The :code:`diaspora-ctl forward` command runs a daemon that reads events from source topics
and forwards them to destination topics, potentially across different drivers. This is useful
for cross-driver event replication (e.g., files to Kafka).

.. code-block:: bash

   diaspora-ctl forward --config <config.toml> --logging <level>

The forwarding daemon is configured via a TOML file specifying drivers and forwarding policies.
For full documentation and example configurations, see :doc:`forwarding`.

