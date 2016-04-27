import json
from math import cos, pi
from heapq import heapify, heappop


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


def simplify(points, maxpts=None, mindist=None, interpolate=False):
    """Simplify location trace by removing geometrically redundant points.

    points -- [{
        "geojson": json.dumps({"coordinates": [lon, lat]}),
        "time": datetime} ...]
    maxpts -- simplify until given number of points remain
    mindist -- omit points contributing no greater than given offset
    interpolate -- use distance from interpolated point, else from line
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

    def dseconds(p0, p1):
        return (p1["time"] - p0["time"]).total_seconds()

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
        fraction = dseconds(p0, p1) / dseconds(p0, p2)
        return distance_point_lineseg(m1, (m0, m2), fraction)

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

    linked = [Dll(x) for x in points]
    for i in range(1, len(linked)):
        linked[i].before = linked[i-1]
    for i in range(len(linked) - 1):
        linked[i].after = linked[i+1]

    f = interpolate and timedist or linedist
    def metric(node):
        return f(node.before.value, node.value, node.after.value)

    heap = [[metric(node), node] for node in linked[1:-1]]
    for node, heap_entry in zip(linked[1:-1], heap):
        node.heap_entry = heap_entry
    heapify(heap)

    while heap:
        m, node = heappop(heap)
        # 3 == the endpoints not in heap, plus the one item popped above
        if ((not maxpts or len(heap) <= maxpts - 3)
                and (not mindist or m > mindist)):
            break
        node.unlink()
        for neighbor in node.before, node.after:
            if hasattr(neighbor, "heap_entry"):
                neighbor.heap_entry[0] = metric(neighbor)
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


def timedelta_str(td):
    """Format timedelta using its two most signifigant units."""
    rv = []
    rem = td.total_seconds()
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


def trace_destinations(points, distance, interval):

    def duration(dest):
        return (dest[1]['time'] - dest[0]['time']).total_seconds()

    # from consequtive overlapping destinations, select the one with longest duration
    best = None
    for dest in destinations_raw(points, distance, interval):
        if not best:
            best = dest
            continue

        if not dest[0]['time'] <= best[1]['time']:
            yield best
            best = dest
            continue

        if duration(dest) > duration(best):
            best = dest

    if best:
        yield best


def destinations_raw(points, distance, interval):

    def dist(p0, p1):
        return get_distance_between_coordinates(
            json.loads(p0["geojson"])["coordinates"],
            json.loads(p1["geojson"])["coordinates"])

    tail = None
    tail_iter = iter(points)

    # make destinations of data snapshot ends, can't know time spent there
    dest = len(points) and (points[0], points[0])

    for head in points:
        # pull tail into max location range
        while not tail or dist(head, tail) > distance:
            if dest:
                yield dest
            dest = None
            tail = next(tail_iter)

        # extend head to min time range
        if (head['time'] - tail['time']).total_seconds() <= interval:
            continue

        # if we make it here that's a valid destination
        dest = tail, head

    if dest:
        yield dest
    elif len(points):
        yield points[-1], points[-1]


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
        streak["properties"].update(feature_properties)
        yield {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    json.loads(x["geojson"])["coordinates"]
                    for x in streak["points"]]},
            "properties": streak["properties"]}
