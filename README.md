# kafka-validate-schema-json-converter

This is a kafka connect based JSON converter plugin which validates the incoming (NOT outgoing) event on a topic against a JSON schema. Currently, the JSON schemas are loaded from resources folder and are packaged inside the jar file. Please make sure the JSON schema file name is same as topic name.

# how to build

> gradle build

# how to test

> gradle test

