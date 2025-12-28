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
    cfg >> packetCount;
    cfg >> receiverId;
  }
};

enum PLPacketType { DATA, ACK };

struct PLPacketHeader {
  unsigned long id;
  size_t len;
  PLPacketType type;
};

struct PLMessageHeader {
  unsigned long long messageId;
  size_t len;
};

/* The packet would have the layout
   [ PLMsgGrpHdr | PLMsgHdr | Msg | PLMsgHdr | Msg | ... ]
 */
struct PLMessageGroupHeader {
  size_t numOfMsgs;
  size_t bufferSize;
};

const size_t PL_MESSAGE_CONTENT_MAX_SIZE = 1024;
const size_t PL_MESSAGE_GROUP_MAX_LENGTH = 8;

inline PLMessageGroupHeader* PLMsgGrpHdr(char *buf) {
  return reinterpret_cast<PLMessageGroupHeader*>(buf);
}

inline PLMessageHeader* PLFirstMsgHdr(PLMessageGroupHeader* grpHdr) {
  return reinterpret_cast<PLMessageHeader*>(reinterpret_cast<char*>(grpHdr) + sizeof(PLMessageGroupHeader));
}

inline char* PLMsgBuf(PLMessageHeader* msgHdr) {
  return reinterpret_cast<char*>(msgHdr) + sizeof(PLMessageHeader);
}

inline PLMessageHeader* PLNextMsgHdr(PLMessageHeader* msgHdr) {
  return reinterpret_cast<PLMessageHeader*>(reinterpret_cast<char*>(msgHdr)+sizeof(PLMessageHeader)+msgHdr->len);
}

size_t serialize_message(string msgstr, unsigned long long msgId, char *buf, size_t bufsize);
void reset_data_packet(PLPacketHeader *pktHdr, PLMessageGroupHeader *grpHdr);
void sender_retransmission(void);
void sender_send_next(void);
void sender_handle_acks(void);
void on_receive_data(PLPacketHeader *pkt);
void on_receive_ack(PLPacketHeader*);
void sender_thread(void);
void receiver_thread(void);

void receiver_broadcast_next_mymessage();

#endif
