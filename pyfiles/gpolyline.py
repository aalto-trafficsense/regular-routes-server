# https://developers.google.com/maps/documentation/utilities/polylinealgorithm

def _subsequences(s):
    item = []
    for c in s:
        o = ord(c) - 63                 # 11-9. ascii ordinal, subtract 63
        item.append(o & 0x1f)           # 8. strip msb from output
        if o & 0x20 == 0:               # 8. has continuation if msb on input
            yield item
            item = []

def _numbers(s):
    for b5s in _subsequences(s):
        val = 0
        for i, b5 in enumerate(b5s):    # 6. eat 5-bit chunks
            val |= b5 << (5*i)          # 7. paste chunks in reverse from right
        sign, val = val & 1, val >> 1   # 4. shift out sign stashed in lsb
        if sign:                        # 5. invert if negative sign
            val ^= 0xffffffff
        val = (val + 0x80000000) % 0x100000000 - 0x80000000 # 3. 2's complement
        yield val / 1e5                 # 2-1. divide by 1e5

def decode_gpolyline(s):
    i = 0
    c = [0, 0]
    for n in _numbers(s):
        c[i] += n
        if i:
            yield tuple(c)
        i = (i + 1) % 2

if __name__ == "__main__":
    import sys
    s = len(sys.argv) > 1 and sys.argv[1] or "_p~iF~ps|U_ulLnnqC_mqNvxq`@"
    print list(decode_gpolyline(s))
