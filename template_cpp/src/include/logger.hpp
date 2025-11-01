#ifndef CS451_LOGGER_H
#define CS451_LOGGER_H

#include <cstring>
#include <fstream>
#include <iostream>
#include <cerrno>

class Logger {
private:
  std::ofstream file;

public:
  Logger(std::string filename) {
    file.open(filename);
    if (file.bad()) {
      std::cerr << "Failed to open " << filename << ": " << strerror(errno) << std::endl;
    }
  }
  ~Logger() {
    file.flush();
    file.close();
  }

  void logln(std::string msg) {
    file << msg << "\n";
  }

  void log(std::string msg) {
    file << msg;
  }
};


#endif
