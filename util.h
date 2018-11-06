#ifndef __UTIL_H__
#define __UTIL_H__

#include <sstream>
#include <iomanip>
#include <vector>
#include <string>

struct StringException : public std::exception {
  std::string str;
  StringException(std::string msg_) : str(msg_) {}

  const char *what() const noexcept { return str.c_str(); }
};

std::string user_readable_size(uint64_t size_) {
  double size(size_);
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(2);
  for (auto str : std::vector<std::string>{"B", "KB", "MB", "GB", "TB"}) {
    if (size < 1024.) {
      oss << size << str;
      return oss.str();
    }
    size /= 1024;
  }
  oss << size << "TB";
  return oss.str();
}

#endif
