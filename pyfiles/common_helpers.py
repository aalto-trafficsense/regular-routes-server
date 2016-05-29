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


def trace_destinations(points, distance, interval):

    # worst-case inaccurate point pair can break up a destination, but only if
    # helped by epsilon of real movement
    points = trace_discard_inaccurate(points, distance / 2)

    # prevent false faraway location points breaking up visits
    points = trace_discard_sidesteps(points, 2)

    points, heads = tee(points, 2)
    dest = head = dend = None

    for point in points:
        if dest is not None:
            dest.append(point)
        if point is dend:
            yield dest
            dest = None
        if head and point_distance(point, head) > distance:
            continue

        for head in heads:
            if point_distance(point, head) > distance:
                break
            if point_interval(point, head) >= interval:
                if dest is None:
                    dest = []
                dend = head

    if dest:
        yield dest


def trace_discard_inaccurate(points, accuracy):
    """Discard points in trace whose accuracy is worse than given radius."""
    return (p for p in points if p["accuracy"] <= accuracy)


def trace_discard_sidesteps(points, factor=2):
    """Discard points in trace that make the distance between their neighbors
    suspiciously long compared to the straight line. This works more completely
    if discard_unmoved is applied first to remove repeats bogus locations."""

    buf = [] # buffer for point and its neighbors
    for p in points:
        buf.append(p)
        if len(buf) < 3:
            continue
        if (point_distance(buf[0], buf[1]) + point_distance(buf[1], buf[2])
                <= factor * point_distance(buf[0], buf[2])):
            yield buf.pop(0)
            continue
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


def trace_regular_destinations(
        points, threshold_distance, threshold_interval):
    """Find regular destinations in location
    trace. Distance and interval thresholds passed on to trace_destinations."""

    dests = [
        {   "coordinates": tuple(
                sum(c) / len(visit)
                for c in zip(*(point_coordinates(p) for p in visit))),
            "visits": [visit]}
        for visit in trace_destinations(
            points, threshold_distance, threshold_interval)]

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
        if distance is None or distance >= threshold_distance:
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
        g["total_time"] = sum(point_interval(v[0], v[-1]) for v in g["visits"])
        coords = []
        entries = []
        exits = []
        for v in g["visits"]:
            coords += [point_coordinates(p) for p in v]
            entries.append((v[0]["time"] - v[0]["time"].replace(
                hour=0, minute=0, second=0, microsecond=0)).total_seconds())
            exits.append((v[-1]["time"] - v[-1]["time"].replace(
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
