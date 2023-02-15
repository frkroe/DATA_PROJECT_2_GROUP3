# Main libraries used in this Project

**apache beam:** Apache Beam is an open-source SDK which allows you to build multiple data pipelines from batch or stream based integrations and run it in a direct or distributed way. You can add various transformations in each pipeline. 
</br></br>
**datetime:** Used to create and manipulate date/time objects. In our case, returning the exact time at the moment of execution and time zone.
</br></br>
**logging:** Provide a flexible framework for emitting log messages from Python programs. This module is widely used by libraries and is often the first go-to point for most developers when it comes to logging.
</br></br>
**random:** Library used to generate random values, in our case, creating data for our mock sensors.
</br></br>
**os:** It is a portable way of interacting with the underlying operating system, allowing your Python code to run on multiple platforms without modification.
</br></br>
**ssl:** SSJ stands for Secure Sockets Layer. It is used to stablish a secure encrypted connection between devices over a network where others could be “spying” on the communication.
</br></br>
**time:** A designated library to interact with time, such as the sleep function which we used to set intervals in our data stream.
</br></br>
**json:** As its name says, this is a library we used to work with JSON files. We used to json.dumps to convert/write python objects into a json string.
</br></br>
**api:** Just like the previous library, this library is also quite self-explanatory. As it’s used to interact with APIs, and in our case, to simulate one iterating rows our data.
</br></br>
**paho.mqtt:** This code provides a client class which enable applications to connect to an MQTT broker to publish messages, and to subscribe to topics and receive published messages. It also provides some helper functions to make publishing one off messages to an MQTT server very straightforward.
</br></br>
**base64:** Base64 is a method of encoding binary data into ASCII text, so that it can be transmitted or stored in a text-based format.
</br></br>
**argparse:** The argparse module makes it easy to write user-friendly command-line interfaces. The program defines what arguments it requires, and argparse will figure out how to parse those out of sys. argv . The argparse module also automatically generates help and usage messages.
</br></br>
**uuid:** The uuid library in Python is a module that provides the ability to generate UUIDs (Universally Unique Identifiers), as well as various utility functions for working with UUIDs.
