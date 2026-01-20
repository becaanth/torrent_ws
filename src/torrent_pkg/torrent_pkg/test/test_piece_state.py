from torrent_pkg.state.piece_state import PieceState

state = PieceState("../../../../deconstructed/1/woody_convoy")

print(state.have(0))   
print(state.have(1))   
print(state.have(2))   
print(state.have(3))   

print(state.missing()) 

print(state.bitfield())  

state.mark_complete(1)

print(state.have(1))     
print(state.missing())   
