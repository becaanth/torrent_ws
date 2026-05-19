import libtorrent as lt
import pprint

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
            pprint.pprint(extras, indent=4)

if __name__ == '__main__':
    path = '/home/asrl/ASRL/vtr3/torrent_ws/scripts/torrent/metadata/metadata.torrent'
    inspect_torrent(path)