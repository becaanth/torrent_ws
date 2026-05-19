import libtorrent as lt
import pprint
import numpy as np
import msgpack

from dataclasses import dataclass, field
from typing import Optional

from rclpy.serialization import deserialize_message, serialize_message
from rosidl_runtime_py.utilities import get_message

@dataclass
class Vertex:
    vertex_id: int

@dataclass
class Edge:
    from_id: int
    to_id: int
    xi: Optional[np.ndarray] = None
    cov: Optional[np.ndarray] = None

    def __repr__(self):
        return f"Edge({self.from_id}) -> ({self.to_id})"
    
def inspect_torrent(path):
    raw = lt.bdecode(open(path, "rb").read())
    info = raw[b"info"]

    print(f"Name:   {info[b'name'].decode()}")
    print(f"Pieces: {len(info[b'pieces']) // 20} x {info.get(b'piece length', '?')} bytes")
    print()

    for file_entry in info.get(b"files", []):
        rel_path = "/".join(p.decode() for p in file_entry[b"path"])
        size = file_entry[b"length"]
        extras = {
            k.decode(): v
            for k, v in file_entry.items()
            if k not in {b"length", b"path", b"attr", b"path.utf-8"}
        }
        print(f"  {rel_path}  ({size} bytes)")
        if extras:
            vertices = msgpack.unpackb(file_entry[b"x-vertices"], raw=False)
            edges    = msgpack.unpackb(file_entry[b"x-edges"],    raw=False)
            pprint.pprint(vertices, indent=4)
            pprint.pprint(edges, indent=8)

if __name__ == '__main__':
    path = '/home/asrl/ASRL/vtr3/torrent_ws/scripts/torrent/metadata/metadata.torrent'
    inspect_torrent(path)