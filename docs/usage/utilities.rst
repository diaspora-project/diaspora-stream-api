Utilities
=========

The Diaspora Stream API comes with the `diaspora-ctl` executable, which can be used
for a number of things listed in this section.

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

Options in parenthesis may be one of the following.

* :code:`format` : :code:`raw` or :code:`json` (default: :code:`raw`). If :code:`json` is
  specified, the line is interpreted as a JSON document. Otherwise it is interpreted as a
  string. Note that because the deamon splits events on new lines, these JSON or strings
  cannot themselves embed new lines.

* :code:`batch_size` : an integer value, representing the batch size the producer must use
  (default is 128).

Note that only metadata can be written into the topic, the data part of events is always left empty.

Consumer command
""""""""""""""""

A consumer command works in a similar manner, however `the specified FIFO device must have been
created first and a process must have opened it in read mode`. The daemon will create a consumer
linked to the topic and write any received metadata into the FIFO device.

Options may again be provided in parenthesis, with currently supported options as follows.

* :code:`batch_size` : an integer value, representing the batch size the producer must use
  (default is 128).

