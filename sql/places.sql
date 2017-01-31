-- Maintain clustering after a cluster was created or updated.
create or replace function places_fixup(cid places.id%type)
returns void as $$
declare
    newcoord places.coordinate%type;
    cluster places%rowtype;
    neighbor places%rowtype;
    eater places.id%type;
    eaten places.id%type;
begin
    -- Update cluster centre according to legend references. Delete if no refs.
    select st_centroid(st_collect(coordinate::geometry)) from leg_ends
        where place = cid and coordinate is not null
        into newcoord;
    if newcoord is null then
        -- Calling trigger may have linked a new node from a null end so...
        update leg_ends set place = null where place = cid;

        delete from places where id = cid;
        return;
    end if;
    update places set coordinate = newcoord where id = cid;

    -- Find nearest neighbor in cluster radius.
    select * from places where id = cid into cluster;
    select * from places
        where st_dwithin(places.coordinate, cluster.coordinate, :clustdist)
            and places.id != cluster.id
        order by st_distance(places.coordinate, cluster.coordinate) asc
        limit 1
        into neighbor;
    if neighbor is null then return; end if;

    -- Find cluster with more legend references to keep.
    select place from leg_ends where place in (cluster.id, neighbor.id)
        group by place order by count(*) desc limit 1 into eater;
    if eater = cluster.id then
        eaten := neighbor.id;
    else
        eaten := cluster.id;
    end if;

    -- Merge weaker cluster into stronger cluster.
    update leg_ends set place = eater where place = eaten;

    -- Delete eaten cluster, fixup merged cluster.
    delete from places where id = eaten;
    perform places_fixup(eater);
end;
$$ language plpgsql volatile returns null on null input;


-- Recluster places after insert/update/delete.
create or replace function leg_ends_changed(old leg_ends, new leg_ends)
returns void as $$
declare
    oneref boolean;
begin
    if old.coordinate is not distinct from new.coordinate then
        return;
    end if;

    -- Don't recreate single-user place from scratch, shift instead
    select count(*) = 1 from leg_ends where place = old.place
        and new.coordinate is not null into oneref;
    if oneref then
        perform places_fixup(old.place);
        return;
    end if;

    perform leg_ends_unlink(old);
    if new.coordinate is not null then
        perform leg_ends_link(new);
    end if;
end;
$$ language plpgsql volatile;


-- Cluster legend.
create or replace function leg_ends_link(legend leg_ends) returns void as $$
declare
    cid leg_ends.place%type;
begin
    insert into places default values returning id into cid;
    update leg_ends set place = cid where id = legend.id;
    perform places_fixup(cid);
end;
$$ language plpgsql volatile;


-- Uncluster legend.
create or replace function leg_ends_unlink(legend leg_ends) returns void as $$
begin
    update leg_ends set place = null where id = legend.id;
    perform places_fixup(legend.place);
end;
$$ language plpgsql volatile;


-- Sadly, can't just use same trigger function as "old" will be unassigned
-- rather than null for inserts, and so forth.
create or replace function leg_ends_deleted() returns trigger as $$
begin perform leg_ends_changed(old, null); return null; end;
$$ language plpgsql volatile;
drop trigger if exists leg_ends_deleted_trigger on leg_ends;
create trigger leg_ends_deleted_trigger after delete on leg_ends
for each row execute procedure leg_ends_deleted();


create or replace function leg_ends_inserted() returns trigger as $$
begin perform leg_ends_changed(null, new); return null; end;
$$ language plpgsql volatile;
drop trigger if exists leg_ends_inserted_trigger on leg_ends;
create trigger leg_ends_inserted_trigger after insert on leg_ends
for each row execute procedure leg_ends_inserted();


create or replace function leg_ends_updated() returns trigger as $$
begin perform leg_ends_changed(old, new); return null; end;
$$ language plpgsql volatile;
drop trigger if exists leg_ends_updated_trigger on leg_ends;
create trigger leg_ends_updated_trigger after update on leg_ends
for each row execute procedure leg_ends_updated();


-- Function that can be called to cluster leg_ends created before places set up.
create or replace function leg_ends_cluster(lim integer) returns void as $$
declare
    legend leg_ends%rowtype;
begin
    for legend in select * from leg_ends
        where coordinate is not null and place is null
        limit lim
    loop
        perform leg_ends_link(legend);
    end loop;
end;
$$ language plpgsql volatile;
