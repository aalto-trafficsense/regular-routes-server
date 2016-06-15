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


def trace_destinations(points, distance, interval):

    # worst-case inaccurate point pair can break up a destination, but only if
    # helped by epsilon of real movement
    points = trace_discard_inaccurate(points, distance / 2)

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
        streak["properties"].update(feature_properties)
        yield {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    json.loads(x["geojson"])["coordinates"]
                    for x in streak["points"]]},
            "properties": streak["properties"]}


def bounds_overlap(bounds0, bounds1):
    for a, b in (bounds0, bounds1), (bounds1, bounds0):
        if (       a["west"] >= b["east"]
                or a["east"] <= b["west"]
                or a["south"] >= b["north"]
                or a["north"] <= b["south"]):
            return False
    return True


def bounds_size(bounds):
    projector = Equirectangular(bounds["west"], bounds["south"])
    p0 = projector.d2m(bounds["west"], bounds["south"])
    p1 = projector.d2m(bounds["east"], bounds["north"])
    return [c1 - c0 for c0, c1 in zip(p0, p1)]


def bounds_union(bounds0, bounds1):
    return {
        "west": min(bounds0["west"], bounds1["west"]),
        "east": max(bounds0["east"], bounds1["east"]),
        "south": min(bounds0["south"], bounds1["south"]),
        "north": max(bounds0["north"], bounds1["north"])}


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
        points, threshold_distance, threshold_interval, maxpts):
    """Get _at most maxpts (not implemented)_ of regular destinations in points
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
        if distance is None or distance >= threshold_distance * 2:
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
        g["total_stay"] = sum(point_interval(v[0], v[-1]) for v in g["visits"])
        coords = []
        entries = []
        exits = []
        for v in g["visits"]:
            coords += [point_coordinates(p) for p in v]
            entries.append((v[0]["time"] - v[0]["time"].replace(
                hour=0, minute=0, second=0, microsecond=0)).total_seconds())
            exits.append((v[-1]["time"] - v[-1]["time"].replace(
                hour=0, minute=0, second=0, microsecond=0)).total_seconds())
        g["avg_coords"] = [
            c / len(coords) for c in map(lambda *x: sum(x), *coords)]

        # XXX these averages are bogus over midnight, natch
        g["avg_entry"] = sum(entries) / len(entries)
        g["avg_exit"] = sum(exits) / len(exits)

    r = 0
    for g in sorted(groups, key=lambda x: x["total_stay"], reverse=True):
        enhm = "%02i:%02i" % divmod(g["avg_entry"]/60, 60)
        exhm = "%02i:%02i" % divmod(g["avg_exit"]/60, 60)
#        bbsz = "%3s\xc3\x97%s" % tuple(int(x) for x in bounds_size(g["bounds"]))
#        print "%s %s %s %.3f/%.3f %8s %s" % (
        r += 1
#        print "{} {} {} {:.3f}/{:.3f}".format(
#            timedelta_str(g["total_stay"]),
#            enhm,
#            exhm,
#            g["coordinates"][1],
#            g["coordinates"][0])

    for r, g in enumerate(sorted(groups, key=lambda x: x["total_stay"], reverse=True)):
        g["duration_rank"] = r + 1

    for r, g in enumerate(sorted(groups, key=lambda x: len(x["visits"]), reverse=True)):
        g["visits_rank"] = r + 1

    return groups


def leftovers(points, distance, interval, maxpts):
    groups = [
        {   "bounds": {
                "west": min(point_coordinates(p)[0] for p in visit),
                "east": max(point_coordinates(p)[0] for p in visit),
                "south": min(point_coordinates(p)[1] for p in visit),
                "north": max(point_coordinates(p)[1] for p in visit)},
            "visits": [visit]}
        for visit in trace_destinations(points, distance, interval)]

    for g in groups:
        print "dest {} {} {} {}\xc3\x97{}".format(
            g["visits"][0][0]["time"], g["visits"][0][-1]["time"],
            len(g["visits"][0]),
            *(int(x) for x in bounds_size(g["bounds"])))

        d = g["visits"][0]
        if d[0]["time"] > d[-1]["time"]:
            for p in d:
                print p["time"]

    print len(groups)

    merged = True
    while merged:
        merged = False
        for g0 in groups:
            i1 = iter(groups)
            for g1 in i1:
                if g0 is g1:
                    break
            for g1 in i1: # items subsequent to g0
                if bounds_overlap(g0["bounds"], g1["bounds"]):
                    g0["bounds"] = bounds_union(g0["bounds"], g1["bounds"])
                    g0["visits"] += g1["visits"]
                    groups.remove(g1)
                    merged = True
                    break
            if merged:
                break

    print len(groups)

    for g in groups:
        g["total_stay"] = sum(point_interval(v[0], v[-1]) for v in g["visits"])
        coords = []
        entries = []
        exits = []
        for v in g["visits"]:
            coords += [point_coordinates(p) for p in v]
            entries.append((v[0]["time"] - v[0]["time"].replace(
                hour=0, minute=0, second=0, microsecond=0)).total_seconds())
            exits.append((v[-1]["time"] - v[-1]["time"].replace(
                hour=0, minute=0, second=0, microsecond=0)).total_seconds())
        g["avg_coords"] = [
            c / len(coords) for c in map(lambda *x: sum(x), *coords)]

        # XXX these averages are bogus over midnight, natch
        g["avg_entry"] = sum(entries) / len(entries)
        g["avg_exit"] = sum(exits) / len(exits)

    for g in sorted(groups, key=lambda x: x["total_stay"], reverse=True):
        enhm = "%02i:%02i" % divmod(g["avg_entry"]/60, 60)
        exhm = "%02i:%02i" % divmod(g["avg_exit"]/60, 60)
        bbsz = "%3s\xc3\x97%s" % tuple(int(x) for x in bounds_size(g["bounds"]))
#        print "%s %s %s %.3f/%.3f %8s %s" % (
        print "{} {} {} {:.3f}/{:.3f} {:8s} {}".format(
            timedelta_str(g["total_stay"]),
            enhm,
            exhm,
            g["avg_coords"][1],
            g["avg_coords"][0],
            bbsz,
            g["bounds"])

    return groups
