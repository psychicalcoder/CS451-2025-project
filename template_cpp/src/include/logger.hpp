#ifndef CS451_LOGGER_H
#define CS451_LOGGER_H

#include <cstring>
#include <fstream>
#include <iostream>
#include <cerrno>
#include <mutex>

class Logger {
private:
  std::mutex lock;
  std::ofstream file;

public:
  Logger(std::string filename) {
    file.open(filename);
    if (file.bad()) {
      std::cerr << "Failed to open " << filename << ": " << strerror(errno) << std::endl;
    }
  }
  ~Logger() {
    std::lock_guard<std::mutex> guard(lock);
    file.flush();
    file.close();
  }

  void logln(std::string msg) {
    std::lock_guard<std::mutex> guard(lock);
    file << msg << "\n";
  }

  void log(std::string msg) {
    std::lock_guard<std::mutex> guard(lock);
    file << msg;
  }
};


#endif
