# Administrative SQL-scripts

These scripts setup empty devices, users and device_data tables and build all related indexes and sequences.

Some instructions for running psql scripts are available in the [regular-routes-server repository wiki](https://github.com/aalto-trafficsense/regular-routes-server/wiki/Terminal-commands-HOWTO).

Chef creates the database and default role (regularroutes) when setting up the server. The map-related tables are created in waypoint generation.

## Init a new empty database, run server, setup for regularroutes

_Note: This assumes that no database exists, i.e. chef was not run._

1) Run `$ ./initdb_server.sh <path-to-new-database-dir>`. The script creates the directory, initiates the database and starts the postgresql server. Leave running, can be closed later with CTRL-C.

1) Switch to another terminal window.

1) Run the init script on psql: `psql -U postgres -f init_rr.sql`

## Restore user, device and device_data from backups

Setup an empty database as above. Additional preparations:
* [Obtain backups](https://github.com/aalto-trafficsense/regular-routes-server/wiki/Terminal-commands-HOWTO) of existing `user`, `device` and `device_data` tables.
* Copy `restore_backup.sh` to the directory with your backups
* Edit the names of the backup files in `restore_backup.sh` accordingly. _Note: Due to cross-references, the restoration order should be as shown in the file._

Execute: `./restore_backup.sh`

Check the result:

    $ psql -U postgres
    postgres=# \d

## Create blank user, device and device_data tables

Setup an empty database as above or DROP the previous tables.

1) Run psql: `$ psql -U postgres`

1) Create the tables: `postgres=# \i create_dudd.sql`

1) Add sequences and indexes: `postgres=# \i index_dudd.sql`

## Repair indexes and sequences of a a partially restored table

Adds the index and sequence information to the following tables:
devices, users, device_data

1) Run psql: `$ psql -U postgres`

1) Add sequences and indexes: `postgres=# \i index_dudd.sql`

_Note: `index_dudd.sql` includes a parameter for setting the last sequence element for all three tables. The script is by default set to work over an empty table, i.e. the number setting entries are commented out. If using `index_dudd.sql` to restore indexes to existing tables, uncomment the command and modify the number accordingly. The last number can be found with e.g. `SELECT MAX(id) FROM device_data ;`._
