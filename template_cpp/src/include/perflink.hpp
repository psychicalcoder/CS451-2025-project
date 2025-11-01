#ifndef PERFLINK_H
#define PERFLINK_H

#include <iostream>
#include <string>
#include <cstring>

#include <unistd.h>
#include <parser.hpp>
#include <vector>
#include <ctime>

using std::ifstream;
using std::string;
using std::vector;

class PerfConfig {
private:
  string configPath;
public:
  unsigned long receiverId;
  unsigned long packetCount;
  
  PerfConfig(string configPath): configPath(configPath) {
    ifstream cfg(configPath);
    cfg >> packetCount >> receiverId;
  }
};

enum PLPacketType { DATA, ACK };

struct PLPacketHeader {
  unsigned long id;
  unsigned long long serial;
  size_t len;
  PLPacketType type;
};

void sender_thread();
void receiver_thread();

#endif
