#!/bin/bash

docker run --env-file env.list --name report-data-provider th2-report-data-provider:latest
