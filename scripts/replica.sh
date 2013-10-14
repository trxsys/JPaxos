#!/bin/sh
java -ea -Djava.util.logging.config.file=logging.properties -cp ../target/classes lsr.paxos.test.EchoServer $*
