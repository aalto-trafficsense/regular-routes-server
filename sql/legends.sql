-- Maintain clustering after a cluster was created or updated.
create or replace function leg_ends_fixup(cid leg_ends.id%type)
returns void as $$
declare
    newcoord leg_ends.coordinate%type;
    cluster leg_ends%rowtype;
    neighbor leg_ends%rowtype;
    eater leg_ends.id%type;
    eaten leg_ends.id%type;
begin
    -- Update cluster centre according to leg references. Delete if no refs.
    select st_centroid(st_collect(c)) from (
            select coordinate_start::geometry c from legs
                where cluster_start = cid and coordinate_start is not null
            union all
            select coordinate_end::geometry c from legs
                where cluster_end = cid and coordinate_end is not null) t0
        into newcoord;
    if newcoord is null then
        -- Calling trigger may have linked a new node from a null end so...
        update legs set cluster_start = null where cluster_start = cid;
        update legs set cluster_end = null where cluster_end = cid;

        delete from leg_ends where id = cid;
        return;
    end if;
    update leg_ends set coordinate = newcoord where id = cid;

    -- Find nearest neighbor in cluster radius.
    select * from leg_ends where id = cid into cluster;
    select * from leg_ends
        where st_dwithin(leg_ends.coordinate, cluster.coordinate, :clustdist)
            and leg_ends.id != cluster.id
            and leg_ends.user_id = cluster.user_id
        order by st_distance(leg_ends.coordinate, cluster.coordinate) asc
        limit 1
        into neighbor;
    if neighbor is null then return; end if;

    -- Find cluster with more leg references to keep.
    select c from (
            select cluster_start c from legs
                where cluster_start in (cluster.id, neighbor.id)
            union all
            select cluster_end c from legs
                where cluster_end in (cluster.id, neighbor.id)) t0
        group by c order by count(*) desc limit 1 into eater;
    if eater = cluster.id then
        eaten := neighbor.id;
    else
        eaten := cluster.id;
    end if;

    -- Merge weaker cluster into stronger cluster.
    update legs set cluster_start = eater where cluster_start = eaten;
    update legs set cluster_end = eater where cluster_end = eaten;

    -- Delete eaten cluster, fixup merged cluster.
    delete from leg_ends where id = eaten;
    perform leg_ends_fixup(eater);
end;
$$ language plpgsql volatile returns null on null input;


-- Recluster leg ends after insert/update/delete.
create or replace function legs_changed(old legs, new legs) returns void as $$
begin
    if old.user_id is distinct from new.user_id then
        perform legs_unlink_start(old);
        perform legs_unlink_end(old);
        perform legs_link_start(new);
        perform legs_link_end(new);
        return;
    end if;

    if old.coordinate_start is distinct from new.coordinate_start then
        perform legs_unlink_start(old);
        perform legs_link_start(new);
    end if;

    if old.coordinate_end is distinct from new.coordinate_end then
        perform legs_unlink_end(old);
        perform legs_link_end(new);
    end if;
end;
$$ language plpgsql volatile;


-- Cluster start of leg.
create or replace function legs_link_start(leg legs)
returns void as $$
declare
    cid legs.cluster_start%type;
begin
    if leg.user_id is null then return; end if;
    insert into leg_ends (user_id) values (leg.user_id) returning id into cid;
    update legs set cluster_start = cid where id = leg.id;
    perform leg_ends_fixup(cid);
end;
$$ language plpgsql volatile;


-- Cluster end of leg.
create or replace function legs_link_end(leg legs)
returns void as $$
declare
    cid legs.cluster_end%type;
begin
    if leg.user_id is null then return; end if;
    insert into leg_ends (user_id) values (leg.user_id) returning id into cid;
    update legs set cluster_end = cid where id = leg.id;
    perform leg_ends_fixup(cid);
end;
$$ language plpgsql volatile;


-- Uncluster start of leg.
create or replace function legs_unlink_start(leg legs) returns void as $$
begin
    update legs set cluster_start = null where id = leg.id;
    perform leg_ends_fixup(leg.cluster_start);
end;
$$ language plpgsql volatile;


-- Uncluster end of leg.
create or replace function legs_unlink_end(leg legs) returns void as $$
begin
    update legs set cluster_end = null where id = leg.id;
    perform leg_ends_fixup(leg.cluster_end);
end;
$$ language plpgsql volatile;


-- Sadly, can't just use same trigger function as "old" will be unassigned
-- rather than null for inserts, and so forth.
create or replace function legs_deleted() returns trigger as $$
begin perform legs_changed(old, null); return null; end;
$$ language plpgsql volatile;
drop trigger if exists legs_deleted_trigger on legs;
create trigger legs_deleted_trigger after delete on legs
for each row execute procedure legs_deleted();


create or replace function legs_inserted() returns trigger as $$
begin perform legs_changed(null, new); return null; end;
$$ language plpgsql volatile;
drop trigger if exists legs_inserted_trigger on legs;
create trigger legs_inserted_trigger after insert on legs
for each row execute procedure legs_inserted();


create or replace function legs_updated() returns trigger as $$
begin perform legs_changed(old, new); return null; end;
$$ language plpgsql volatile;
drop trigger if exists legs_updated_trigger on legs;
create trigger legs_updated_trigger after update on legs
for each row execute procedure legs_updated();


-- Function that can be called to cluster legs created before leg_ends set up.
create or replace function legs_cluster() returns void as $$
declare
    leg legs%rowtype;
begin
    for leg in select * from legs
        where user_id is not null
          and coordinate_start is not null
          and cluster_start is null
    loop
        perform legs_link_start(leg);
    end loop;

    for leg in select * from legs
        where user_id is not null
          and coordinate_end is not null
          and cluster_end is null
    loop
        perform legs_link_end(leg);
    end loop;
end;
$$ language plpgsql volatile;
