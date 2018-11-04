#ifndef __BYTES_H__
#define __BYTES_H__

#include <vector>
#include <string>
#include <iostream>

#include <kj/common.h>
#include <kj/string.h>

#include <capnp/serialize.h>

//typedef std::vector<uint8_t> Bytes;
struct StringException : public std::exception {
  std::string str;
  StringException(std::string msg_) : str(msg_) {}

  const char *what() const noexcept { return str.c_str(); }
};

struct Bytes : public std::vector<uint8_t> {
  Bytes() {}
 Bytes(int n) : std::vector<uint8_t>(n) {}
 Bytes(std::string &s) : std::vector<uint8_t>(s.size()) { memcpy(&(*this)[0], &s[0], s.size()); }
 Bytes(const char *c) : std::vector<uint8_t>() { std::string s(c); resize(s.size()); memcpy(&(*this)[0], &s[0], s.size()); }
 Bytes(capnp::Data::Reader r) : ::Bytes(r.begin(), r.end()) {}
 //Bytes(capnp::Data::Reader const &r) : ::Bytes(r.begin(), r.end()) {}
  Bytes &operator=(capnp::Data::Reader const &r) {*this = Bytes(r.begin(), r.end()); return *this;}

    template <typename T>
      Bytes(T *c, T *e) : std::vector<uint8_t>(c, e) {}

    template <typename T>
      Bytes(const T *c, size_t n) : std::vector<uint8_t>(reinterpret_cast<uint8_t const *>(c), reinterpret_cast<uint8_t const *>(c + n)) {};
 Bytes(unsigned char *b, unsigned char *e) : std::vector<uint8_t>(b, e) {}

  operator std::string() const {
    std::string s(size(), 0);
    memcpy(&s[0], &(*this)[0], size());
    return s;
  }

  std::string str() const {
    std::string s(size(), 0);
    memcpy(&s[0], &(*this)[0], size());
    return s;
  }
  
  kj::ArrayPtr<kj::byte> kjp() {
    return kj::ArrayPtr<kj::byte>(&(*this)[0], size());
  }

  kj::ArrayPtr<::capnp::word const> kjwp() {
    return kj::ArrayPtr<::capnp::word const>((::capnp::word const*) &(*this)[0], (size()+1)/2);
  }

   template <typename T = uint8_t*>
     T ptr() {
     return reinterpret_cast<T>(&(*this)[0]);
   }
   
   operator kj::ArrayPtr<kj::byte const>() {
     //void bla() {
     return kjp();
   }

   operator kj::ArrayPtr<::capnp::word const>() {
     //void bla() {
     return kjwp();
   }

};

inline std::ostream &operator<<(std::ostream &out, Bytes const &b) {
  for (auto h : b)
    fprintf(stderr, "%x", h);
  return out;
  //return std::cout << b.str();
}

#endif
