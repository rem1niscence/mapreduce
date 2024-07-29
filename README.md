# Distributed Map Reduce implementation

# What is it

The [MapReduce paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf), written by Jeffrey Dean and Sanjay Ghemawat, introduces a programming model and an associated implementation for processing and generating large-scale datasets. It provides a simple and flexible framework for parallel processing of data across a cluster of computers. The MapReduce model consists of two main functions: the `Map` function, which processes input data and produces a set of intermediate key-value pairs, and the `Reduce` function, which merges the intermediate values associated with the same intermediate key. This paper revolutionized the field of distributed computing and laid the foundation for big data processing frameworks like [Apache Hadoop](https://hadoop.apache.org/).


## Implementation

in the seminal MapReduce paper by Google. This project was developed as part of an assignment for the MIT course 6.5840: Distributed Systems.

Overview

Our implementation satisfies all the requirements of the assignment and successfully passes the test script provided by the course. The setup includes a Docker Compose file to run the program with a simulated distributed filesystem between instances, emulating the distributed nature of the original MapReduce system.

### Features

*	**Complete MapReduce Framework**: Implements both the Map and Reduce phases as described in the MapReduce paper.
*	**Distributed File System Simulation**: Uses Docker Compose to simulate a distributed filesystem across multiple instances.
*	**Tested and Verified**: Passes all the provided test scripts to ensure correctness and reliability.

## Getting Started

### Prerequisites

* Docker
* Docker Compose
* Golang

### Usage


```sh
$ docker compose up --build
```

The Docker setup will automatically distribute the MapReduce tasks across the simulated distributed filesystem. You can modify the input data and MapReduce functions by editing the files in the [cmd/testdata](cmd/testdata) folder. You can also modify the map/reduce function used in the compose file, these are located in the [mapfuncs](mapfuncs) folder.
