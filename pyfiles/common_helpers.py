import json

from datetime import timedelta
from heapq import heapify, heappop, heappush
from itertools import tee
from math import cos, pi


def get_distance_between_coordinates(coord1, coord2):
    # from: http://stackoverflow.com/questions/1253499/simple-calculations-for-working-with-lat-lon-km-distance
    # The approximate conversions are:
    # Latitude: 1 deg = 110.574 km
    # Longitude: 1 deg = 111.320*cos(latitude) km

    x_diff = (coord1[0] - coord2[0]) * 110320 * cos(coord2[1] / 180 * pi)
    y_diff = (coord1[1] - coord2[1]) * 110574

    distance = (x_diff * x_diff + y_diff * y_diff)**0.5
    return distance


class Equirectangular:
    """Rough equirectangular lat/lon degrees vs local metres projection."""

    latscale = 111194.9 # from mean radius

    def __init__(self, lon, lat):
        self.lon = lon
        self.lat = lat
        self.lonscale = Equirectangular.latscale * cos(pi * lat / 180)

    def d2m(self, lon, lat):
        y = (lat - self.lat) * Equirectangular.latscale
        x = (lon - self.lon) * self.lonscale
        return x, y

    def m2d(self, x, y):
        lat = self.lat + y / Equirectangular.latscale
        lon = self.lon + x / self.lonscale
        return lon, lat


def point_coordinates(p):
    return json.loads(p["geojson"])["coordinates"]


def point_distance(p0, p1):
    return get_distance_between_coordinates(
        point_coordinates(p0), point_coordinates(p1))


def point_interval(p0, p1):
    return (p1["time"] - p0["time"]).total_seconds()


def simplify_geometry(
        points,
        maxpts=None,
        mindist=None,
        interpolate=False,
        keep_activity=False):
    """Simplify location trace by removing geometrically redundant points.

    points -- [{
        "geojson": json.dumps({"coordinates": [lon, lat]}),
        "time": datetime} ...]
    maxpts -- simplify until given number of points remain
    mindist -- omit points contributing no greater than given offset
    interpolate -- use distance from interpolated point, else from line
    keep_activity -- keep last moved>mindist point of each continuous activity
    """

    # avoid building heap if input already conformant
    if (not points or not mindist and (not maxpts or len(points) <= maxpts)):
        return points

    ballpark = json.loads(points[0]['geojson'])['coordinates']
    projector = Equirectangular(*ballpark)

    def distance_point_lineseg(p, l, par=None):
        """Distance of point p from line segment l.

        p -- point, list of coordinates
        l -- line segment, pair of lists of coordinates
        par -- reference point along line segment 0..1, nearest if None given

        Equivalently with 'shapely':
        Point(p).distance(LineString(l))
        Point(p).distance(LineString(l).interpolate(par, True))
        """

        dim = range(len(p))
        p_l0 = [p[i] - l[0][i] for i in dim]
        l1_l0 = [l[1][i] - l[0][i] for i in dim]
        dot = sum(a * b for a, b in zip(p_l0, l1_l0))
        lsq = sum(a * a for a in l1_l0)
        if par is None:
            par = max(0, min(1, lsq and dot / lsq))
        ref = [l[0][i] + par * l1_l0[i] for i in dim]
        p_ref = [p[i] - ref[i] for i in dim]
        return sum(p_ref[i]**2 for i in dim)**.5

    def linedist(p0, p1, p2):
        """Distance of p1 from line segment between p0 and p2."""
        m0, m1, m2 = (
            projector.d2m(*json.loads(x['geojson'])['coordinates'])
            for x in (p0, p1, p2))
        return distance_point_lineseg(m1, (m0, m2))

    def timedist(p0, p1, p2):
        """Distance of p1 from its time interpolation between p0 and p2."""
        m0, m1, m2 = (
            projector.d2m(*json.loads(x['geojson'])['coordinates'])
            for x in (p0, p1, p2))
        fraction = point_interval(p0, p1) / point_interval(p0, p2)
        return distance_point_lineseg(m1, (m0, m2), fraction)

    dist = interpolate and timedist or linedist

    def keeping_activity(p0, p1, p2):
        """Sortable (bool, dist) metric"""
        changes = p1["activity"] != p2["activity"]
        moved = point_distance(p0, p1) > mindist
        return (changes and moved, dist(p0, p1, p2))

    metric = keep_activity and keeping_activity or dist
    minmetric = keep_activity and (False, mindist) or mindist

    return simplify(points, metric, maxpts, minmetric)


def simplify(points, metric, maxpts=None, minmetric=None):
    """Remove points in order of lowest metric(predecessor, point, successor)
    until it reaches minmetric, and number of points is no greater than
    maxpts."""

    class Dll:
        def __init__(self, value, before=None, after=None):
            self.value = value
            self.before = before
            self.after = after
        def unlink(self):
            if self.before:
                self.before.after = self.after
            if self.after:
                self.after.before = self.before

    def node_metric(node):
        return metric(node.before.value, node.value, node.after.value)

    linked = [Dll(x) for x in points]
    for i in range(1, len(linked)):
        linked[i].before = linked[i-1]
    for i in range(len(linked) - 1):
        linked[i].after = linked[i+1]

    heap = [[node_metric(node), node] for node in linked[1:-1]]
    for node, heap_entry in zip(linked[1:-1], heap):
        node.heap_entry = heap_entry
    heapify(heap)

    while heap:
        m, node = heappop(heap)
        # 3 == the endpoints not in heap, plus the one item popped above
        if ((not maxpts or len(heap) <= maxpts - 3)
                and (not minmetric or m > minmetric)):
            break
        node.unlink()
        for neighbor in node.before, node.after:
            if hasattr(neighbor, "heap_entry"):
                neighbor.heap_entry[0] = node_metric(neighbor)
        heapify(heap)

    node = linked[0]
    rv = []
    while node:
        rv.append(node.value)
        node = node.after
    return rv


def dict_groups(dicts, keys):
    """Group consecutive dicts that are equal in given keys."""
    gkey = None
    group = []
    for d in dicts:
        dkey = dict((k, d[k]) for k in keys if k in d)
        if dkey != gkey:
            if gkey is not None:
                yield gkey, group
            gkey = dkey
            group = []
        group.append(d)
    if gkey is not None:
        yield gkey, group


def trace_partition_movement(
        points,
        distance,
        interval,
        break_interval=None,
        include_inaccurate=True):
    """Wrapper to optionally split stops at data gaps greater than
    break_interval."""

    pll = break_interval \
          and trace_split_sparse(points, break_interval) \
          or [points]
    for pl in pll:
        for seg in trace_partition_movement_nobreak(
                pl, distance, interval, include_inaccurate):
            yield seg


def trace_partition_movement_nobreak(
        points, distance, interval, include_inaccurate=True):
    """Wrapper to optionally restore inaccurate and sharp points which may be
    useful for activity selection, but are discarded by the callee."""

    if not include_inaccurate:
        for x in trace_partition_movement_dropsome(points, distance, interval):
            yield x
        return

    allpts, inpts = tee(points, 2)
    outseg = []
    for mov, inseg in trace_partition_movement_dropsome(
            inpts, distance, interval):
        p = next(allpts)
        while p is not inseg[0]:
            outseg.append(p)
            p = next(allpts)
        if outseg:
            yield None, outseg # undecided points between stop/move segments
            outseg = []
        while p is not inseg[-1]:
            outseg.append(p)
            p = next(allpts)
        outseg.append(p) # last point of segment
        yield mov, outseg
        outseg = []
    outseg = [p for p in allpts]
    if outseg:
        yield None, outseg


def trace_partition_movement_dropsome(points, distance, interval):
    """Partition location trace into moving and stationary segments as a
    sequence of (bool moving, list points). A stationary segment is where the
    trace moves less than the given distance in the given time interval.

    Destination entry and exit are refined to the points maximizing the
    difference between the distances traveled in the prior and subsequent
    intervals, clamped to the distance to avoid faraway window edge effects.

    The former uses interval measure on distance window edges; the other way
    around, looping over a longer distance and back in the given interval would
    be misidentifed as stationary.

    The latter uses distance measure on interval window edges; the other way
    around, points on the exit path would interfere with entry detection and
    vice versa, resulting in more or less random knee points.

    Low accuracy or otherwise suspicious points are discarded.

    XXX could rewrite this to just emit (bool moving, datetime start, datetime
    end) since now the caller reconstructs the inaccurate or otherwise
    suspicious points back into the output where they may be useful for
    activity recognition."""

    # worst-case inaccurate point pair can break up a destination, but only if
    # helped by epsilon of real movement
    points = trace_discard_inaccurate(points, distance / 2)

    # prevent false faraway location points breaking up visits
    points = trace_discard_sidesteps(points, 2)

    inf = float("inf")
    entryend = exitend = None
    entrymax = exitmax = -inf
    moveseg, stopseg, nextseg = [], [], []

    points, heads, itails, iheads = tee(points, 4)
    head = next(heads, None) # distance lookahead window for stop condition
    itail = next(itails, None) # interval back window for entry/exit refinement
    ihead = None # point inside interval lookahead window
    next_ihead = next(iheads, None) # point outside interval lookahead window

    def refine_entry():
        refine_exit()
        moveseg.extend(stopseg)
        stopseg[:] = [moveseg.pop()] # current point first in stop

    def refine_exit():
        stopseg.extend(nextseg)
        nextseg[:] = []

    for point in points:
        nextseg.append(point)

        # Adjust two-pane time window used for entry/exit refinement.
        while point_interval(itail, point) > interval:
            itail = next(itails, None)
        while next_ihead and point_interval(point, next_ihead) <= interval:
            ihead = next_ihead
            next_ihead = next(iheads, None) # the overextended point

        # Track knees i.e. sharpest speed change at entry and exit. Clamp to
        # neighborhood of interest to avoid faraway window edge effects.
        stretch = (min(distance, point_distance(point, ihead))
                 - min(distance, point_distance(itail, point)))

        # Refine exit when in a stop.
        if exitend and stretch >= exitmax: # >= prefer later when equal
            exitmax = stretch
            refine_exit()

        # Find valid windows while advancing head until out of range.
        while head and point_distance(point, head) <= distance:

            # When stop condition is valid, do stuff.
            if point_interval(point, head) >= interval:

                # On first validity on a stop, set up entry refinement window.
                if exitend is None:
                    entrymax = -inf
                    entryend = head

                    # If at gap or start of trace, don't refine entry further.
                    if point is itail:
                        entrymax = inf
                        refine_entry()

                # Reset exit refinement window.
                exitmax = -inf
                exitend = head

            head = next(heads, None)

        # At the end of the entry window, emit the prior move segment.
        if point is entryend:
            entryend = None
            if moveseg:
                yield True, moveseg
            moveseg = []

        # At end of stop condition validity, emit the stop segment.
        if point is exitend:

            # If followed by gap or end of trace, set exit here.
            if point is ihead:
                refine_exit()

            exitend = None
            if stopseg:
                yield False, stopseg
            stopseg = []

        # Refine entry point when in entry window.
        if (entryend and -stretch > entrymax): # > prefer earlier when equal
            entrymax = -stretch
            refine_entry()
            exitmax = -inf # reset exit refinement

    if exitend:
        stopseg += nextseg
    else:
        moveseg += nextseg

    if moveseg:
        yield True, moveseg
    if stopseg:
        yield False, stopseg


def trace_destinations(points, distance, interval, include_inaccurate=True):
    """Find stationary subsequences in location trace"""
    for mov, seg in trace_partition_movement(
            points, distance, interval, None, include_inaccurate):
        if mov is False:
            yield seg


def trace_discard_inaccurate(points, accuracy):
    """Discard points in trace whose accuracy is worse than given radius."""
    return (p for p in points if p["accuracy"] <= accuracy)


def trace_discard_sidesteps(points, factor=2):
    """Discard points in trace that make the distance between their neighbors
    suspiciously long compared to the straight line."""

    d = point_distance
    def badness(p0, p1, p2):
        hyp = d(p0, p2)
        return hyp and (d(p0, p1) + d(p1, p2)) / hyp or float("inf")

    # sidestep can make prior point look bad so drop only if next is not worse
    buf = []
    for p in points:
        buf.append(p)
        if len(buf) < 4:
            continue
        badness1 = badness(*buf[0:3])
        badness2 = badness(*buf[1:4])
        if badness1 > factor and badness1 > badness2:
            buf.pop(1)
            continue
        yield buf.pop(0)

    # last three points
    if len(buf) == 3 and badness(*buf) > factor:
        buf.pop(1)

    for p in buf:
        yield p


def trace_discard_unmoved(points):
    """Discard points in trace where the accuracy radius includes the
    previously accepted point, so no clear evidence of movement."""

    previous = None
    for p in points:
        if previous:
            if point_distance(previous, p) <= p["accuracy"]:
                continue
            yield previous
        previous = p
    if previous:
        yield previous


def trace_linestrings(points, keys=(), feature_properties=()):
    """Render sequence of points as geojson linestring features.

    points -- [{geojson: json.dumps({coordinates: [lon, lat]})} ...]
    keys -- render separate linestring when these values change in points
    feature_properties -- dict added to each feature's properties object
    """

    # collect by same activity for line coloring
    groups = dict_groups(points, keys)
    streaks = [{"properties": g[0], "points": g[1]} for g in groups]

    # start line from last point of prior streak
    for i in range(1, len(streaks)):
        streaks[i]["points"].insert(0, streaks[i-1]["points"][-1])

    for streak in streaks:
        if len(streak["points"]) < 2:
            continue # one point does not make a line
        streak["properties"].update(feature_properties)
        yield {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    point_coordinates(x) for x in streak["points"]]},
            "properties": streak["properties"]}


def datetime_range_str(dt0, dt1, resolution=2, formats=(
        "%Y-%m-%d", " ", "%H:%M", ":", "%S.%u")):
    """Format datetime range start and end according to formats without repeated
    parts in the end string. %u stands for the microsecond part. Every other
    format is considered a separator. The number of formats included is minimum
    resolution and at least one divergent, all if resolution -1."""

    s0 = []
    s1 = []
    diverged = False
    issep = True
    sep = ""
    for f in formats:
        issep = not issep
        if issep:
            sep = f
            continue
        if not resolution and diverged:
            break
        resolution -= 1
        f0, f1 = [f.replace("%u", str(dt.microsecond)) for dt in (dt0, dt1)]
        p0 = dt0.strftime(f0)
        p1 = dt1.strftime(f1)
        if p0 != p1:
            diverged = True
        if not diverged:
            s0 += [sep, p0]
            continue
        s0 += [sep, p0]
        s1 += [sep, p1]
    return "".join(s0[1:]), "".join(s1[1:])


def timedelta_str(td):
    """Format timedelta (or seconds) using its two most signifigant units."""
    rv = []
    rem = td.total_seconds() if isinstance(td, timedelta) else td
    for first, notfirst, mul in [
            ("%.2fs", "%02is", 60),
            ("%im",   "%02im", 60),
            ("%ih",   "%02ih", 24),
            ("%id",   "%02id", 30),
            ("%im",   "%02im", 12),
            ("%iy",   "%iy",   None) ]:
        div, rem = mul and divmod(rem, mul) or (None, rem)
        if not div:
            rv = [ first % rem ] + rv
            break
        rv = [ notfirst % rem ] + rv
        rem = div
    return "%6s" % ''.join(rv[:2])


def vector_average(vv):
    """Calculate average from iterable of vectors."""
    c = None
    for i, v in enumerate(vv):
        c = v if c is None else tuple((i*a+b)/(i+1) for (a, b) in zip(c, v))
    return c


def trace_center(points):
    """Calculate rough center for given points. Not correct across dateline."""
    return vector_average(point_coordinates(p) for p in points)


def trace_regular_destinations(points, threshold_distance, threshold_interval):
    """Find regular destinations in location
    trace. Distance and interval thresholds passed on to trace_destinations."""

    # With refined entry/exit, the visit centres should not be too biased by
    # the entry/exit traces, so using double cluster distance should not too
    # often lead to two adjacent stops clustering into one destination.
    cluster_distance = threshold_distance * 2

    dests = [
        {   "coordinates": tuple(
                sum(c) / len(visit)
                for c in zip(*(point_coordinates(p) for p in visit))),
            "time_start": visit[0]["time"],
            "time_end": visit[-1]["time"]}
        for visit in trace_destinations(
            points, threshold_distance, threshold_interval, False)]

    return stop_clusters(dests, cluster_distance)


def stop_clusters(stops, cluster_distance):
    """Cluster stops {coordinates: (x, y), time_start: datetime, time_end:
    datetime} into regular destinations."""

    def dest_dist(d0, d1):
        return get_distance_between_coordinates(
            d0["coordinates"], d1["coordinates"])

    def dest_weighted_center(*destinations):
        weighted = [
            tuple(len(d["visits"]) * c for c in d["coordinates"])
            for d in destinations]
        total = sum(len(d["visits"]) for d in destinations)
        return tuple(sum(c) / total for c in zip(*weighted))

    def heapitem(d0, dests):
        """Find nearest neighbor for d0 as sortable [distance, nearest, d0]"""
        return (min([dest_dist(d0, d1),
                     d1] for d1 in dests if d1 is not d0)
                  + [d0])

    # Start by making each stop a destination with a single visit.
    dests = [{
            "coordinates": x["coordinates"],
            "visits": [{
                "time_start": x["time_start"],
                "time_end": x["time_end"]}]}
        for x in stops]

    heap = [[None, None, d] for d in dests]
    d0 = d1 = merged = None
    while len(heap) > 1:
        for item in heap:
            # rescan nearest where nearest was merged away, or not yet set
            if item[1] in (None, d0, d1):
                item[:] = heapitem(item[2], (x[2] for x in heap))
                continue

            # update others where merged now nearest
            if item[2] is not merged:
                distance = dest_dist(item[2], merged)
                if item[0] > distance:
                    item[0:2] = distance, merged

        # arrange heap, pop out one end of shortest edge
        heapify(heap)
        distance, d1, d0 = item = heappop(heap)

        # if shortest edge is long enough, unpop and stop
        if distance is None or distance >= cluster_distance:
            heappush(heap, item) # unspill the milk
            break

        # replace other end with merged destination
        merged = {
            "coordinates": dest_weighted_center(d0, d1),
            "visits": d0["visits"] + d1["visits"]}
        for i in range(len(heap)):
            if heap[i][2] is d1:
                heap[i] = [None, None, merged]
                break

    groups = [x[2] for x in heap]
    for g in groups:
        g["total_time"] = sum(
            (v["time_end"] - v["time_start"]).total_seconds()
            for v in g["visits"])
        entries = []
        exits = []
        for v in g["visits"]:
            entries.append((v["time_start"] - v["time_start"].replace(
                hour=0, minute=0, second=0, microsecond=0)).total_seconds())
            exits.append((v["time_end"] - v["time_end"].replace(
                hour=0, minute=0, second=0, microsecond=0)).total_seconds())

    for r, g in enumerate(sorted(
            groups,
            key=lambda x: (x["total_time"], len(x["visits"])),
            reverse=True)):
        g["total_time_rank"] = r + 1

    for r, g in enumerate(sorted(
            groups,
            key=lambda x: (len(x["visits"]), x["total_time"]),
            reverse=True)):
        g["visits_rank"] = r + 1

    return groups


def trace_split_sparse(points, interval):
    """"Split location trace where interval between points exceeds given
    threshold interval."""

    segment = []
    for p in points:
        if segment and point_interval(segment[-1], p) > interval:
            yield segment
            segment = []
        segment.append(p)
    if segment:
        yield segment
