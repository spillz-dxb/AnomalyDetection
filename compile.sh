#!/bin/bash

mvn clean package

scp target/AnomalyDetection-1.0-SNAPSHOT.jar golabadmin:~/anomalydetection