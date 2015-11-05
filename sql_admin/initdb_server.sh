#!/bin/bash

# Initialize a new database and start a server on it

if [ "$#" -ne 1 ]; then
    Echo "One parameter required: Path to the directory (created by this script) for the new database."
    exit 1
fi

# Create the directory
mkdir $1

# Initialize a database
initdb -D $1 -U postgres

# Start postgres on it
postgres -D $1
