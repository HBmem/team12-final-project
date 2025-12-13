#!/bin/bash

#python3 faas_runner.py -f functions/j-service1.json functions/j-service2.json functions/j-service3.json -e experiments/java-tests/projectService1Java100.json experiments/java-tests/projectService2Java100.json experiments/java-tests/projectService3Java100.json

#python3 faas_runner.py -f functions/j-service1.json functions/j-service2.json functions/j-service3.json -e experiments/java-tests/projectService1Java1000.json experiments/java-tests/projectService2Java1000.json experiments/java-tests/projectService3Java1000.json

python3 faas_runner.py -f functions/j-service1.json functions/j-service2.json functions/j-service3.json -e experiments/java-tests/projectService1Java5000.json experiments/java-tests/projectService2Java5000.json experiments/java-tests/projectService3Java5000.json

python3 faas_runner.py -f functions/j-service1.json functions/j-service2.json functions/j-service3.json -e experiments/java-tests/projectService1Java10000.json experiments/java-tests/projectService2Java10000.json experiments/java-tests/projectService3Java10000.json

python3 faas_runner.py -f functions/j-service1.json functions/j-service2.json functions/j-service3.json -e experiments/java-tests/projectService1Java50000.json experiments/java-tests/projectService2Java50000.json experiments/java-tests/projectService3Java50000.json

python3 faas_runner.py -f functions/j-service1.json functions/j-service2.json functions/j-service3.json -e experiments/java-tests/projectService1Java100000.json experiments/java-tests/projectService2Java100000.json experiments/java-tests/projectService3Java100000.json

python3 faas_runner.py -f functions/j-service1.json functions/j-service2.json functions/j-service3.json -e experiments/java-tests/projectService1Java500000.json experiments/java-tests/projectService2Java500000.json experiments/java-tests/projectService3Java500000.json

python3 faas_runner.py -f functions/j-service1.json functions/j-service2.json functions/j-service3.json -e experiments/java-tests/projectService1Java1000000.json experiments/java-tests/projectService2Java1000000.json experiments/java-tests/projectService3Java1000000.json
