#!/bin/sh

mvn clean package -DskipTests -Pdist

if [ $? -ne "0" ]; then
    echo "mvn package failed"
    exit 2;
fi
