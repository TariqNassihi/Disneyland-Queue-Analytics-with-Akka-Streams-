# Disneyland Queue Analytics with Akka Streams

## Overview
This project implements a **stream-processing system** that analyzes queue time data from attractions in Disneyland Paris.

The system processes a CSV dataset containing ride queue records and computes several analytics such as ride statistics, wait-time variations, and dynamic skip-the-line pricing.

The implementation is built using **Akka Streams** and follows a **Pipe-and-Filter architecture**, where data flows through a sequence of processing stages that transform and analyze the data stream. :contentReference[oaicite:0]{index=0}
