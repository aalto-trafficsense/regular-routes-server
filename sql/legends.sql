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


-- Cluster start of leg.
create or replace function legs_cluster_start(legid legs.id%type)
returns void as $$
declare
    cid legs.cluster_start%type;
begin
    insert into leg_ends default values returning id into cid;
    update legs set cluster_start = cid where id = legid;
    perform leg_ends_fixup(cid);
end;
$$ language plpgsql volatile;


-- Cluster end of leg.
create or replace function legs_cluster_end(legid legs.id%type)
returns void as $$
declare
    cid legs.cluster_end%type;
begin
    insert into leg_ends default values returning id into cid;
    update legs set cluster_end = cid where id = legid;
    perform leg_ends_fixup(cid);
end;
$$ language plpgsql volatile;


-- When a leg is deleted, fixup clusters where ref disappeared.
create or replace function legs_deleted() returns trigger as $$
begin
    perform leg_ends_fixup(OLD.cluster_start);
    perform leg_ends_fixup(OLD.cluster_end);
    return null;
end;
$$ language plpgsql volatile;

drop trigger if exists legs_deleted_trigger on legs;
create trigger legs_deleted_trigger after delete on legs
for each row execute procedure legs_deleted();


-- When a leg is inserted, cluster its ends.
create or replace function legs_inserted() returns trigger as $$
begin
    perform legs_cluster_start(NEW.id);
    perform legs_cluster_end(NEW.id);
    return null;
end;
$$ language plpgsql volatile;

drop trigger if exists legs_inserted_trigger on legs;
create trigger legs_inserted_trigger after insert on legs
for each row execute procedure legs_inserted();


-- If a leg's end coordinates changed, relink clusters appropriately.
create or replace function legs_updated() returns trigger as $$
begin
    if NEW.coordinate_start is distinct from OLD.coordinate_start then
        perform legs_cluster_start(NEW.id);
        perform leg_ends_fixup(OLD.cluster_start);
    end if;
    if NEW.coordinate_end is distinct from OLD.coordinate_end then
        perform legs_cluster_end(NEW.id);
        perform leg_ends_fixup(OLD.cluster_end);
    end if;
    return null;
end;
$$ language plpgsql volatile;

drop trigger if exists legs_updated_trigger on legs;
create trigger legs_updated_trigger after update on legs
for each row execute procedure legs_updated();
