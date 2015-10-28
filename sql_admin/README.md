# Administrative SQL-scripts

These scripts setup empty devices, users and device_data tables and build all related indexes and sequences.

Some instructions for running psql scripts are available in the [regular-routes-server repository wiki](https://github.com/aalto-trafficsense/regular-routes-server/wiki/Terminal-commands-HOWTO). 

## create_dudd.sql

Creates the following tables:
devices, users, device_data

Chef creates the database when setting up the server. The map-related tables are created in waypoint generation. This script should be run after waypoint generation and chef setup scripts, when initiating a new server from scratch.

## index_dudd.sql

Adds the index and sequence information to the following tables:
devices, users, device_data

Expected to be used:
* after `create_dudd.sql` to complete the setup
* after restoring data, in case index- and sequence info were not restored from the dump.

_Note: The script includes a parameter for setting the last sequence element for all three tables. The script is by default set to work over an empty table, i.e. the number setting entries are commented out. If using `index_dudd.sql` to restore indexes to existing tables, uncomment the command and modify the number accordingly. The last number can be found with e.g. `SELECT MAX(id) FROM device_data ;`._


