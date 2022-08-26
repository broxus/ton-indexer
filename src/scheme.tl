// Generic stuff
////////////////////////////////////////////////////////////////////////////////

---types---

int ? = Int;
long ? = Long;
double ? = Double;
string ? = String;
object ? = Object;
function ? = Function;
bytes data:string = Bytes;
true = True;
boolTrue = Bool;
boolFalse = Bool;

vector {t:Type} # [ t ] = Vector t;

int128 4*[ int ] = Int128;
int256 8*[ int ] = Int256;


// Broadcast
////////////////////////////////////////////////////////////////////////////////

---types---

tonNode.blockSignature who:int256 signature:bytes = tonNode.BlockSignature;

tonNode.blockBroadcast
    id:tonNode.blockIdExt
    catchain_seqno:int
    validator_set_hash:int
    signatures:(vector tonNode.blockSignature)
    proof:bytes
    data:bytes
        = tonNode.Broadcast;
tonNode.externalMessageBroadcast message:tonNode.externalMessage = tonNode.Broadcast;
tonNode.connectivityCheckBroadcast pub_key:int256 padding:bytes = tonNode.Broadcast;

tonNode.externalMessage data:bytes = tonNode.ExternalMessage;


// RPC
////////////////////////////////////////////////////////////////////////////////

---types---

tonNode.blockDescriptionEmpty = tonNode.BlockDescription;
tonNode.blockDescription id:tonNode.blockIdExt = tonNode.BlockDescription;

tonNode.preparedProofEmpty = tonNode.PreparedProof;
tonNode.preparedProof = tonNode.PreparedProof;
tonNode.preparedProofLink = tonNode.PreparedProof;

tonNode.prepared = tonNode.Prepared;
tonNode.notFound = tonNode.Prepared;

tonNode.preparedState = tonNode.PreparedState;
tonNode.notFoundState = tonNode.PreparedState;

tonNode.keyBlocks blocks:(vector tonNode.blockIdExt) incomplete:Bool error:Bool = tonNode.KeyBlocks;

tonNode.dataFull id:tonNode.blockIdExt proof:bytes block:bytes is_link:Bool = tonNode.DataFull;
tonNode.dataFullEmpty = tonNode.DataFull;

tonNode.data data:bytes = tonNode.Data;

tonNode.archiveNotFound = tonNode.ArchiveInfo;
tonNode.archiveInfo id:long = tonNode.ArchiveInfo;

tonNode.capabilities version:int capabilities:long = tonNode.Capabilities;

---functions---

tonNode.getNextBlockDescription prev_block:tonNode.blockIdExt = tonNode.BlockDescription;
tonNode.prepareBlockProof block:tonNode.blockIdExt allow_partial:Bool = tonNode.PreparedProof;
tonNode.prepareKeyBlockProof block:tonNode.blockIdExt allow_partial:Bool = tonNode.PreparedProof;
tonNode.prepareBlock block:tonNode.blockIdExt = tonNode.Prepared;
tonNode.preparePersistentState block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt = tonNode.PreparedState;
tonNode.prepareZeroState block:tonNode.blockIdExt = tonNode.PreparedState;
tonNode.getNextKeyBlockIds block:tonNode.blockIdExt max_size:int = tonNode.KeyBlocks;
tonNode.downloadNextBlockFull prev_block:tonNode.blockIdExt = tonNode.DataFull;
tonNode.downloadBlockFull block:tonNode.blockIdExt = tonNode.DataFull;
tonNode.downloadBlock block:tonNode.blockIdExt = tonNode.Data;
tonNode.downloadPersistentStateSlice block:tonNode.blockIdExt masterchain_block:tonNode.blockIdExt offset:long max_size:long = tonNode.Data;
tonNode.downloadZeroState block:tonNode.blockIdExt = tonNode.Data;
tonNode.downloadBlockProof block:tonNode.blockIdExt = tonNode.Data;
tonNode.downloadKeyBlockProof block:tonNode.blockIdExt = tonNode.Data;
tonNode.downloadBlockProofLink block:tonNode.blockIdExt = tonNode.Data;
tonNode.downloadKeyBlockProofLink block:tonNode.blockIdExt = tonNode.Data;
tonNode.getArchiveInfo masterchain_seqno:int = tonNode.ArchiveInfo;
tonNode.getArchiveSlice archive_id:long offset:long max_size:int = tonNode.Data;

tonNode.getCapabilities = tonNode.Capabilities;
