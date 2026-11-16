"""
CityHash128WithSeed of CityHash 1.0.2 used by ClickHouse, as clickhouse-cityhash ignores the seed.
"""

_MASK = (1 << 64) - 1
_K0 = 0xC3A5C85C97CB3127
_K1 = 0xB492B66FBE98F273
_K2 = 0x9AE16A3B2F90404F
_K3 = 0xC949D7C7509E6557
_KMUL = 0x9DDFEA08EB382D69


def city_hash128_with_seed(data: bytes, seed: tuple[int, int]) -> tuple[int, int]:
    """
    Hash data with a 128-bit seed; both are pairs of 64-bit halves (first, second).
    """
    if len(data) < 128:
        return _city_murmur(data, seed)

    s = 0
    length = len(data)
    x, y = seed
    z = length * _K1 & _MASK
    v0 = (_rotate(y ^ _K1, 49) * _K1 + _fetch64(data, s)) & _MASK
    v1 = (_rotate(v0, 42) * _K1 + _fetch64(data, s + 8)) & _MASK
    w0 = (_rotate(y + z & _MASK, 35) * _K1 + x) & _MASK
    w1 = _rotate(x + _fetch64(data, s + 88) & _MASK, 53) * _K1 & _MASK

    while True:
        for _ in range(2):
            x = _rotate(x + y + v0 + _fetch64(data, s + 16) & _MASK, 37) * _K1 & _MASK
            y = _rotate(y + v1 + _fetch64(data, s + 48) & _MASK, 42) * _K1 & _MASK
            x ^= w1
            y ^= v0
            z = _rotate(z ^ w0, 33)
            v0, v1 = _weak_hash_len32_with_seeds(
                data, s, v1 * _K1 & _MASK, x + w0 & _MASK
            )
            w0, w1 = _weak_hash_len32_with_seeds(data, s + 32, z + w1 & _MASK, y)
            z, x = x, z
            s += 64
        length -= 128
        if length < 128:
            break

    y = (y + _rotate(w0, 37) * _K0 + z) & _MASK
    x = (x + _rotate(v0 + z & _MASK, 49) * _K0) & _MASK
    tail_done = 0
    while tail_done < length:
        tail_done += 32
        y = (_rotate(y - x & _MASK, 42) * _K0 + v1) & _MASK
        w0 = (w0 + _fetch64(data, s + length - tail_done + 16)) & _MASK
        x = (_rotate(x, 49) * _K0 + w0) & _MASK
        w0 = (w0 + v0) & _MASK
        v0, v1 = _weak_hash_len32_with_seeds(data, s + length - tail_done, v0, v1)

    x = _hash_len16(x, v0)
    y = _hash_len16(y, w0)
    return (
        _hash_len16(x + v1 & _MASK, w1) + y & _MASK,
        _hash_len16(x + w1 & _MASK, y + v1 & _MASK),
    )


def _city_murmur(data: bytes, seed: tuple[int, int]) -> tuple[int, int]:
    """
    Hash data shorter than 128 bytes.
    """
    length = len(data)
    a, b = seed
    if length <= 16:
        a = _shift_mix(a * _K1 & _MASK) * _K1 & _MASK
        c = (b * _K1 + _hash_len0to16(data)) & _MASK
        d = _shift_mix(a + (_fetch64(data, 0) if length >= 8 else c) & _MASK)
    else:
        c = _hash_len16(_fetch64(data, length - 8) + _K1 & _MASK, a)
        d = _hash_len16(b + length & _MASK, c + _fetch64(data, length - 16) & _MASK)
        a = (a + d) & _MASK
        for s in range(0, length - 16, 16):
            a ^= _shift_mix(_fetch64(data, s) * _K1 & _MASK) * _K1 & _MASK
            a = a * _K1 & _MASK
            b ^= a
            c ^= _shift_mix(_fetch64(data, s + 8) * _K1 & _MASK) * _K1 & _MASK
            c = c * _K1 & _MASK
            d ^= c
    a = _hash_len16(a, c)
    b = _hash_len16(d, b)
    return a ^ b, _hash_len16(b, a)


def _hash_len0to16(data: bytes) -> int:
    """
    Hash data of up to 16 bytes into 64 bits.
    """
    length = len(data)
    if length > 8:
        a = _fetch64(data, 0)
        b = _fetch64(data, length - 8)
        return _hash_len16(a, _rotate(b + length & _MASK, length)) ^ b
    if length >= 4:
        a = _fetch32(data, 0)
        return _hash_len16(length + (a << 3) & _MASK, _fetch32(data, length - 4))
    if length > 0:
        y = data[0] + (data[length >> 1] << 8)
        z = length + (data[length - 1] << 2)
        return _shift_mix((y * _K2 ^ z * _K3) & _MASK) * _K2 & _MASK
    return _K2


def _weak_hash_len32_with_seeds(data: bytes, s: int, a: int, b: int) -> tuple[int, int]:
    """
    Hash 32 bytes of data at the given offset with two seeds.
    """
    w = _fetch64(data, s)
    x = _fetch64(data, s + 8)
    y = _fetch64(data, s + 16)
    z = _fetch64(data, s + 24)
    a = (a + w) & _MASK
    b = _rotate(b + a + z & _MASK, 21)
    c = a
    a = (a + x + y) & _MASK
    b = (b + _rotate(a, 44)) & _MASK
    return a + z & _MASK, b + c & _MASK


def _hash_len16(u: int, v: int) -> int:
    """
    Hash 128 bits into 64 bits.
    """
    a = (u ^ v) * _KMUL & _MASK
    a ^= a >> 47
    b = (v ^ a) * _KMUL & _MASK
    b ^= b >> 47
    return b * _KMUL & _MASK


def _rotate(value: int, shift: int) -> int:
    """
    Rotate 64 bits right.
    """
    return (value >> shift | value << (64 - shift)) & _MASK


def _shift_mix(value: int) -> int:
    """
    Mix the high bits into the low ones.
    """
    return value ^ value >> 47


def _fetch64(data: bytes, offset: int) -> int:
    """
    Read 64 bits in little endian.
    """
    return int.from_bytes(data[offset : offset + 8], "little")


def _fetch32(data: bytes, offset: int) -> int:
    """
    Read 32 bits in little endian.
    """
    return int.from_bytes(data[offset : offset + 4], "little")
