#!/usr/bin/env bash

tr -cd '\11\12\15\40-\176' < $1 > $2
