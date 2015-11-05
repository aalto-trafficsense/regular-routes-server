#!/bin/bash

# Restore backups into an empty DB
# Assuming that initdb_server.sh has been run => Database has been initialized and postgres is running

# Restore backups ** FIX NAMES **
pg_restore -U regularroutes -d postgres users-2015-09-30.tar
pg_restore -U regularroutes -d postgres devices-2015-09-30.tar
pg_restore -U regularroutes -d postgres device_data-2015-09-16.tar
