#!/bin/bash

for container in ci/*; do
    docker build $container
done
