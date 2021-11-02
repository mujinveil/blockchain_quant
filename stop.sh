#!/bin/sh
 ps -ef | grep python | cut -c 9-15| xargs kill -s 9