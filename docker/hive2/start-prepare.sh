#!/bin/bash

echo "##############################"
echo "##      START PREPARE       ##"
echo "##############################"

echo "##############################"
echo "##     PATCH HADOOP CONF    ##"
echo "##############################"

cp /var/input/hadoop/conf/* /opt/hadoop/etc/hadoop/
