all:
	clang++ -fuse-ld=lld -std=c++17 -lcapnp -lkj `pkg-config --libs --cflags glib-2.0`  `pkg-config --libs --cflags gio-2.0` -lb2 -llz4 -llmdb  files.capnp.c++ archiver.cc

