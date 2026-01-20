def bits_to_string(bitfield: list[bool]) -> str:
    bits = [int(b) for b in bitfield]

    # pad to multiple of 8
    pad = (8 - (len(bits) % 8)) % 8
    bits.extend([0] * pad)

    chars = []
    for i in range(0, len(bits), 8):
        byte = bits[i:i+8]
        value = 0
        for b in byte:
            value = (value << 1) | b
        chars.append(chr(value))

    return "".join(chars)

def string_to_bits(s: str) -> list[bool]:
    bits = []
    for c in s:
        byte = ord(c)
        bits.extend([(byte >> i) & 1 for i in range(7, -1, -1)])
    return [bool(b) for b in bits]
