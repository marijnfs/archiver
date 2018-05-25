#include <capnp/message.h>
#include <capnp/serialize.h>
#include <capnp/serialize-packed.h>


::capnp::MallocMessageBuilder cap_message;
auto builder = cap_message.initRoot<cap::Recording>();
auto cap_data = messageToFlatArray(cap_message);
auto cap_bytes = cap_data.asBytes();
bytes->resize(cap_bytes.size());
memcpy(&(*bytes)[0], &cap_bytes[0], cap_bytes.size());



Bytes kjwp
::capnp::FlatArrayMessageReader reader(bytes.kjwp());
variables.clear();
objects.clear();
triggers.clear();
snaps.clear();
index_map.clear();

auto rec = reader.getRoot<cap::Recording>();

