import struct
from datetime import datetime, timedelta
from pydantic import validate_call

@validate_call(config=dict(arbitrary_types_allowed=True))
def decode_datetime(bytes8):
    """MSSQL datetime = 2 little-endian ints: days since 1900 + ticks fraction."""
    days, ticks = struct.unpack("<ii", bytes8)
    base = datetime(1900, 1, 1)
    return base + timedelta(days=days, milliseconds=ticks * 0.00390625)

@validate_call(config=dict(arbitrary_types_allowed=True))
def decode_numeric(data):
    """MSSQL numeric/decimal internal storage."""
    precision = data[0]
    scale = data[1]
    sign = data[2]  # 0 = negative, 1 = positive
    magnitude = data[3:]

    # Convert little-endian magnitude bytes into integer
    val = int.from_bytes(magnitude, byteorder="little", signed=False)

    if scale > 0:
        val = val / (10 ** scale)

    if sign == 0:   # negative
        val = -val

    return val


@validate_call(config=dict(arbitrary_types_allowed=True))
def decode_rowlog_contents(blob: bytes, schema):
    if blob is None:
        return None

    pos = 0
    result = {}

    # 1) Status bits
    pos += 2

    # 2) Column count
    col_count = struct.unpack("<H", blob[pos:pos+2])[0]
    pos += 2

    # 3) Null bitmap
    null_bytes = (col_count + 7) // 8
    nullmap = blob[pos:pos+null_bytes]
    pos += null_bytes

    # 4) Process each column from schema
    # NOTE: Users table = all columns are variable-length except int/numeric/datetime/bit
    # MSSQL uses: first fixed-length, then variable-length section

    # A) Decode fixed-length columns first
    fixed_vals = {}
    var_cols = []
    for col in schema:
        t = col["type"]
        name = col["name"]

        if t in ["int", "bigint", "datetime", "bit", "numeric"]:
            # check null bit
            idx = col["ordinal"] - 1
            null_bit = (nullmap[idx // 8] >> (idx % 8)) & 1
            if null_bit == 1:
                fixed_vals[name] = None
                continue

            if t == "int":
                val = struct.unpack("<i", blob[pos:pos+4])[0]
                pos += 4

            elif t == "bigint":
                val = struct.unpack("<q", blob[pos:pos+8])[0]
                pos += 8

            elif t == "bit":
                val = blob[pos]
                pos += 1

            elif t == "datetime":
                val = decode_datetime(blob[pos:pos+8])
                pos += 8

            elif t == "numeric":
                # numeric storage length varies; SQL Server stores length in first byte of value
                # read length first
                length = blob[pos]  # includes precision, scale, sign, magnitude
                val = decode_numeric(blob[pos+1 : pos+1+length])
                pos += 1 + length

            fixed_vals[name] = val
        else:
            var_cols.append(col)

    # B) Variable-length column count
    var_col_count = struct.unpack("<H", blob[pos:pos+2])[0]
    pos += 2

    # C) Offset array
    offsets = []
    for _ in range(var_col_count):
        off = struct.unpack("<H", blob[pos:pos+2])[0]
        pos += 2
        offsets.append(off)

    # D) Data section for all var-length blobs
    var_data_start = pos

    # decode each variable-length column
    for idx, col in enumerate(var_cols):
        name = col["name"]
        t = col["type"]

        start = var_data_start + (offsets[idx - 1] if idx > 0 else 0)
        end = var_data_start + offsets[idx]

        raw = blob[start:end]

        # Null check: MSSQL uses 0xFFFF offset for NULL
        if offsets[idx] == 0xFFFF:
            result[name] = None
            continue

        if t in ["varchar", "char"]:
            # variable-length strings in RowLog have a length prefix
            strlen = raw[0]
            s = raw[1:1+strlen].decode(errors="ignore")
            result[name] = s
        else:
            result[name] = raw

    # Merge fixed & variable
    result.update(fixed_vals)

    return result
