#!/bin/bash

while true; do docker stats --no-stream --no-trunc >> $1; done
