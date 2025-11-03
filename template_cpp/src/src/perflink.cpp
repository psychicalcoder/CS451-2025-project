#include <cerrno>
#include <chrono>
#include <cstddef>
#include <exception>
#include <logger.hpp>
#include <mutex>
#include <new>
#include <parser.hpp>
#include <perflink.hpp>
#include <queue>
#include <set>
#include <thread>
#include <udp.hpp>
#include <iostream>
#include <map>
#include <unordered_set>
#include <cstring>

using namespace std;

struct pl_msgsig {
  unsigned long id;
  unsigned long long serial;
};

static bool operator<(const pl_msgsig &a, const pl_msgsig &b) {
  return a.id == b.id ? a.serial < b.serial : a.id < b.id;
}

mutex ack_queue_lock;
queue<uint64_t> ack_queue;

typedef chrono::time_point<chrono::high_resolution_clock> timeout_point;

extern UDP *udp;
extern Logger *logger;
extern unsigned long myid;

set<pl_msgsig> delivered;

struct pl_message {
  unsigned long receiverId;
  unique_ptr<char[]> msg; // serialized content, include msghdr and msg
  size_t msgsize; // serialized content, include msghdr and msg
  timeout_point timeout;
  int retry;
};

// MessageId -> Message
map<uint64_t, pl_message> messagePool;

// receiverId -> MessageId set
map<unsigned long, unordered_set<uint64_t>> pendingMessages;

size_t pendingMessagesCount;
size_t idealPendingCount;
size_t sentCount;
uint64_t lastSerial;

extern unsigned long target_send_cnt;
extern unsigned long target_id;

size_t serialize_message(string msgstr, unsigned long long msgId, char *buf, size_t bufsize) {
  size_t contentlen = msgstr.length()+1;
  PLMessageHeader *msghdr = reinterpret_cast<PLMessageHeader*>(buf);
  char *contentbuf = buf+sizeof(PLMessageHeader);
  size_t contentbufcap = bufsize-sizeof(PLMessageHeader);
  size_t exactcontentlen = min(contentlen, contentbufcap);
  memcpy(contentbuf, msgstr.c_str(), exactcontentlen);
  msghdr->len = exactcontentlen;
  msghdr->messageId = msgId;
  return exactcontentlen + sizeof(PLMessageHeader);
}

void reset_data_packet(PLPacketHeader *pktHdr, PLMessageGroupHeader *grpHdr) {
  pktHdr->id = myid;
  pktHdr->len = sizeof(PLMessageGroupHeader);
  pktHdr->type = DATA;
  grpHdr->bufferSize = 0;
  grpHdr->numOfMsgs = 0;
}

void sender_retransmission() {
  static char packetBuffer[sizeof(PLPacketHeader)+
			   sizeof(PLMessageGroupHeader)+
			   PL_MESSAGE_GROUP_MAX_LENGTH*
			     (sizeof(PLMessageHeader)+PL_MESSAGE_CONTENT_MAX_SIZE)];

  PLPacketHeader *pktHdr = reinterpret_cast<PLPacketHeader*>(packetBuffer);
  PLMessageGroupHeader *grpHdr = reinterpret_cast<PLMessageGroupHeader*>(packetBuffer+sizeof(PLPacketHeader));

  timeout_point now = chrono::high_resolution_clock::now();

  for (auto &receiverGrps : pendingMessages) {
    unsigned long receiverId = receiverGrps.first;
    reset_data_packet(pktHdr, grpHdr);
        
    unordered_set<uint64_t> &msgs = receiverGrps.second;
    size_t msgcnt = 0;
    char *slot = packetBuffer + (sizeof(PLPacketHeader) + sizeof(PLMessageGroupHeader));
    
    for (uint64_t msgId : msgs) {
      auto ret = messagePool.find(msgId);
      if (ret == messagePool.end()) continue;
      pl_message &msgInfo = ret->second;
      if (msgInfo.timeout > now) continue;

      // cout << "retransmit " << msgId << endl;
      
      // if timeout, need retransmission
      memcpy(slot, msgInfo.msg.get(), msgInfo.msgsize);

      grpHdr->bufferSize += msgInfo.msgsize;
      grpHdr->numOfMsgs++;
      pktHdr->len += msgInfo.msgsize;
      slot = slot + msgInfo.msgsize;
      
      msgcnt++;
      msgInfo.timeout = now+200ms;
      if (msgcnt == PL_MESSAGE_GROUP_MAX_LENGTH) {
	int err = udp->netsend(receiverId, packetBuffer, sizeof(PLPacketHeader)+pktHdr->len);
	if (err == -EWOULDBLOCK || err == -ENOBUFS) return;
	reset_data_packet(pktHdr, grpHdr);
	msgcnt = 0;
	slot = packetBuffer + (sizeof(PLPacketHeader) + sizeof(PLMessageGroupHeader));
      }
    }
    
    if (msgcnt > 0) {
      int err = udp->netsend(receiverId, packetBuffer, sizeof(PLPacketHeader)+pktHdr->len);
      if (err == -EWOULDBLOCK || err == -ENOBUFS) return;
      reset_data_packet(pktHdr, grpHdr);
    }
  }
}

void sender_send_next() {
  static char msgbuf[sizeof(PLMessageHeader)+PL_MESSAGE_CONTENT_MAX_SIZE];
  
  auto &targetPool = pendingMessages[target_id];

  if (pendingMessagesCount >= idealPendingCount) idealPendingCount += PL_MESSAGE_GROUP_MAX_LENGTH;
 
  while (pendingMessagesCount < idealPendingCount && sentCount < target_send_cnt) {
    ++lastSerial;
    ++pendingMessagesCount;

    PLMessageHeader* msghdr = reinterpret_cast<PLMessageHeader*>(msgbuf);
    char* contentbuf = msgbuf+sizeof(PLMessageHeader);
    // start content
    ++sentCount;
    int slen = snprintf(contentbuf, PL_MESSAGE_CONTENT_MAX_SIZE-1, "%lu", sentCount);
    contentbuf[slen] = '\0';
    // terminator is not included, so manually add it.
    slen++;
    // end content
    msghdr->messageId = lastSerial;
    msghdr->len = static_cast<size_t>(slen);
    size_t msgsize = sizeof(PLMessageHeader)+msghdr->len;
    try {
      unique_ptr<char[]> msg(new char[msgsize]);
      memcpy(msg.get(), msgbuf, msgsize);
      messagePool[lastSerial] = pl_message{
	target_id,
	std::move(msg),
	msgsize,
	timeout_point(),
	0
      };
    } catch(std::bad_alloc& ba) {
      break;
    }
    targetPool.insert(lastSerial);

    // cout << "b " << contentbuf << endl;
    logger->logln(string("b ") + contentbuf);
  }
}

void sender_handle_acks() {
  ack_queue_lock.lock();

  while (!ack_queue.empty()) {
    uint64_t msgId = ack_queue.front();
    ack_queue.pop();
    auto ref = messagePool.find(msgId);
    if (ref != messagePool.end()) {
      messagePool.erase(ref);
      // cout << "ack " << msgId << endl;
      --pendingMessagesCount;
    }

    unordered_set<uint64_t> &targetPool = pendingMessages[ref->second.receiverId];
    auto ref2 = targetPool.find(msgId);
    if (ref2 != targetPool.end()) {
      targetPool.erase(ref2);
    }
  }
  
  ack_queue_lock.unlock();
}

void sender_thread() {
  pendingMessagesCount = 0;
  lastSerial = 0;
  sentCount = 0;
  idealPendingCount = 40;
  
  while (true) {
    sender_handle_acks();
    sender_send_next();
    sender_retransmission();
    this_thread::sleep_for(50ms);
  }
}

void onReceiveData(PLPacketHeader *pkt) {
  static char logbuf[PL_MESSAGE_CONTENT_MAX_SIZE+128];
  static char ackbuf[sizeof(PLPacketHeader)+(sizeof(uint64_t)*PL_MESSAGE_GROUP_MAX_LENGTH)];
  PLMessageGroupHeader *grpHdr = reinterpret_cast<PLMessageGroupHeader*>(reinterpret_cast<char*>(pkt)+sizeof(PLPacketHeader));
  PLMessageHeader *msgHdr = PLFirstMsgHdr(grpHdr);
  unsigned long senderId = pkt->id;

  PLPacketHeader *ackpkt = reinterpret_cast<PLPacketHeader*>(ackbuf);
  uint64_t *curPkt = reinterpret_cast<uint64_t*>(ackbuf+sizeof(PLPacketHeader));
  size_t ackcnt = 0;
  ackpkt->id = myid;
  ackpkt->len = 0;
  ackpkt->type = ACK;
  
  for (size_t i = 0; i < grpHdr->numOfMsgs; i++) {
    if (delivered.count(pl_msgsig{senderId, msgHdr->messageId}) == 0) {
      // trigger deliver
      int offset = snprintf(logbuf, sizeof(logbuf), "d %lu ", senderId);
      offset += snprintf(logbuf+offset, min(sizeof(logbuf)-static_cast<size_t>(offset)-1,
					    min(msgHdr->len,
						PL_MESSAGE_CONTENT_MAX_SIZE)),
			 "%s",
			 PLMsgBuf(msgHdr));
      logbuf[offset] = '\0';
      logger->logln(string(logbuf));
      // cout << string(logbuf) << endl;
      delivered.insert(pl_msgsig{senderId, msgHdr->messageId});
    }
    if (ackcnt < PL_MESSAGE_GROUP_MAX_LENGTH) {
	curPkt[ackcnt] = msgHdr->messageId;
	ackpkt->len += sizeof(uint64_t);
	++ackcnt;
    }
    msgHdr = PLNextMsgHdr(msgHdr);
  }
  udp->netsend(senderId, ackbuf, sizeof(PLPacketHeader)+ackpkt->len);
}

void onReceiveAck(PLPacketHeader *pkt) {
  size_t num_of_ack = pkt->len / sizeof(uint64_t);
  uint64_t *ackid = reinterpret_cast<uint64_t*>(reinterpret_cast<char*>(pkt)+sizeof(PLPacketHeader));
  if (num_of_ack <= 0) return;
  
  ack_queue_lock.lock();
  for (size_t i = 0; i < num_of_ack; i++) {
    // cout << "recv ack " << ackid[i] << endl;
    ack_queue.push(ackid[i]);
  }
  ack_queue_lock.unlock();
}


void receiver_thread() {
  static char buffer[4096];
  while (true) {
    size_t len = udp->netrecv(buffer, sizeof(buffer));
    PLPacketHeader *header = reinterpret_cast<PLPacketHeader *>(buffer);
    char *msg = buffer + sizeof(PLPacketHeader);
    if (header->type == DATA) {
      onReceiveData(header);
    } else if (header->type == ACK) {
      onReceiveAck(header);
    } else {
      // error
    }
  }
}
