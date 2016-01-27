# A Note (from Jesse)

These files were there to start with:

	delete_duplicate_device_data.sql
	snapping.sql

I added these:

    create_averaged_location.sql      # <-- averaged location stored in this table
    make_average_table.sql            # <-- ... and this is the script to fill it with data from device_data
    select_averaged_location.sql      # <-- ... to select from it
    update_averaged_location.sql      # <-- ... to update it (without building from scratch)
    make_predictions_table.sql        # <-- predictions stored in this table
    list_active_devices.sql           # <-- this script determines which devices are considered 'active'
    make_cluster_table.sql            # <-- personal nodes stored in this table

... some of which need to be run manually, others are called automatically by the python code.
