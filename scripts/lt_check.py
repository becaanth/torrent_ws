import libtorrent as lt
import sys

print(f"File: {lt.__file__}")
# Check the pro features
has_ed = hasattr(lt, 'ed25519_create_keypair')
ses = lt.session()
has_dht = hasattr(ses, 'dht_put_mutable_item')

print(f"Ed25519: {has_ed}")
print(f"DHT Put: {has_dht}")