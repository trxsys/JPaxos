#!/bin/sh
java -ea -Djava.util.logging.config.file=logging.properties -cp ../target/jpaxos-1.0.jar lsr.paxos.test.SimplifiedMapServer $*
