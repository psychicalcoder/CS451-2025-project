#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <exception>
#include <iostream>
#include <logger.hpp>
#include <map>
#include <mutex>
#include <new>
#include <parser.hpp>
#include <perflink.hpp>
#include <queue>
#include <set>
#include <stdexcept>
#include <thread>
#include <udp.hpp>
#include <unordered_set>
#include <list>
#include <broadcast.hpp>

using namespace std;

struct PLMsgSig {
  unsigned long id;
  unsigned long long serial;
};

static bool operator<(const PLMsgSig &a, const PLMsgSig &b) {
  return a.id == b.id ? a.serial < b.serial : a.id < b.id;
}

mutex ack_queue_lock;
queue<uint64_t> ack_queue;

typedef chrono::time_point<chrono::high_resolution_clock> timeout_point;

extern UDP *udp;
extern Logger *logger;
extern unsigned long myid;

set<PLMsgSig> delivered;

struct PLTask {
  unsigned long receiverId;
  unique_ptr<char[]> msg; // serialized content, include msghdr and msg
  size_t msgsize;         // serialized content, include msghdr and msg
  timeout_point timeout;
  int retry;
};

// MessageId -> Message
map<uint64_t, PLTask> message_pool;

// receiverId -> MessageId set
map<unsigned long, set<uint64_t>> pending_messages;

size_t pending_messages_count;
size_t ideal_pending_count;
size_t sent_count;
uint64_t last_serial;

extern unsigned long target_send_cnt;
// extern unsigned long target_id;

MessageFactory msg_factory;
extern FIFOBroadcast *fifo;


size_t serialize_message(string msgstr, unsigned long long msgId, char *buf,
                         size_t bufsize) {
  size_t contentlen = msgstr.length() + 1;
  PLMessageHeader *msghdr = reinterpret_cast<PLMessageHeader *>(buf);
  char *contentbuf = buf + sizeof(PLMessageHeader);
  size_t contentbufcap = bufsize - sizeof(PLMessageHeader);
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
  static char
      packetBuffer[sizeof(PLPacketHeader) + sizeof(PLMessageGroupHeader) +
                   PL_MESSAGE_GROUP_MAX_LENGTH *
                       (sizeof(PLMessageHeader) + PL_MESSAGE_CONTENT_MAX_SIZE)];

  PLPacketHeader *pktHdr = reinterpret_cast<PLPacketHeader *>(packetBuffer);
  PLMessageGroupHeader *grpHdr = reinterpret_cast<PLMessageGroupHeader *>(
      packetBuffer + sizeof(PLPacketHeader));

  timeout_point now = chrono::high_resolution_clock::now();

  // round robin in case wasting bandwidth to a dead receiver
  list<pair<set<uint64_t>*, set<uint64_t>::iterator>> current_progress;
  for (auto &receiverGrps : pending_messages) {
    auto it = receiverGrps.second.begin();
    if (it != receiverGrps.second.end()) {
      current_progress.push_back(make_pair(&receiverGrps.second, it));
    }
  }

  while (!current_progress.empty()) {
    for (auto &receiver : current_progress) {
      if (receiver.second == receiver.first->end()) continue;

      set<uint64_t> &msgs = *receiver.first;
      size_t msgcnt = 0;
      char *slot =
	packetBuffer + (sizeof(PLPacketHeader) + sizeof(PLMessageGroupHeader));
      set<uint64_t>::iterator &msgit = receiver.second;

      uint64_t receiverId = 2147483647;
      reset_data_packet(pktHdr, grpHdr);

      while (msgit != msgs.end()) {
	uint64_t msgId = *msgit;
	msgit++;

	auto ret = message_pool.find(msgId);
	if (ret == message_pool.end())
	  continue;
	
	PLTask &msgInfo = ret->second;
	if (msgInfo.timeout > now)
	  continue;

	// cout << "PL retransmit " << msgId << endl;

	// if timeout, need retransmission
	memcpy(slot, msgInfo.msg.get(), msgInfo.msgsize);

	// if (receiverId != 2147483647 && receiverId != msgInfo.receiverId) perror("receiverId is not consistent");
	receiverId = msgInfo.receiverId;

	grpHdr->bufferSize += msgInfo.msgsize;
	grpHdr->numOfMsgs++;
	pktHdr->len += msgInfo.msgsize;
	slot = slot + msgInfo.msgsize;

	msgcnt++;
	msgInfo.timeout = now + 200ms;
	if (msgcnt == PL_MESSAGE_GROUP_MAX_LENGTH) {
	  break;
	}
      }
      if (msgcnt > 0) {	
	int err = udp->netsend(receiverId, packetBuffer,
			       sizeof(PLPacketHeader) + pktHdr->len);
	if (err == -EWOULDBLOCK || err == -ENOBUFS) {
	  cout << "retransmission failed" << endl;
	  return;
	}
      }
    }
    auto it = current_progress.begin();
    while (it != current_progress.end()) {
      if (it->second == it->first->end()) {
	it = current_progress.erase(it);
      } else {
	it++;
      }
    }
  }
}

/*
void sender_send_next() {
  static char msgbuf[sizeof(PLMessageHeader) + PL_MESSAGE_CONTENT_MAX_SIZE];

  auto &targetPool = pending_messages[target_id];

  if (pending_messages_count >= ideal_pending_count)
    ideal_pending_count += PL_MESSAGE_GROUP_MAX_LENGTH;

  while (pending_messages_count < ideal_pending_count &&
         sent_count < target_send_cnt) {
    ++last_serial;
    ++pending_messages_count;

    PLMessageHeader *msghdr = reinterpret_cast<PLMessageHeader *>(msgbuf);
    char *contentbuf = msgbuf + sizeof(PLMessageHeader);
    // start content
    ++sent_count;
    int slen =
        snprintf(contentbuf, PL_MESSAGE_CONTENT_MAX_SIZE - 1, "%lu",
sent_count); contentbuf[slen] = '\0';
    // terminator is not included, so manually add it.
    slen++;
    // end content
    msghdr->messageId = last_serial;
    msghdr->len = static_cast<size_t>(slen);
    size_t msgsize = sizeof(PLMessageHeader) + msghdr->len;
    try {
      unique_ptr<char[]> msg(new char[msgsize]);
      memcpy(msg.get(), msgbuf, msgsize);
      message_pool[last_serial] =
          PLTask{target_id, std::move(msg), msgsize, timeout_point(), 0};
    } catch (std::bad_alloc &ba) {
      break;
    }
    targetPool.insert(last_serial);

    // cout << "b " << contentbuf << endl;
    logger->logln(string("b ") + contentbuf);
  }
}
*/

void sender_send_next() {
  static char msgbuf[sizeof(PLMessageHeader) + PL_MESSAGE_CONTENT_MAX_SIZE];

  int T = 8;
  while (T--) {
    string content = fifo->next_message();
    if (content.length() == 0) return;

    for (uint64_t targetId : fifo->hosts) {
      if (targetId == myid) continue; // reduce network flow
      PLMessageHeader *msghdr = reinterpret_cast<PLMessageHeader*>(msgbuf);
      char *contentbuf = msgbuf + sizeof(PLMessageHeader);
      msghdr->messageId = last_serial;
      msghdr->len = content.length()+1;
      strncpy(contentbuf, content.c_str(), PL_MESSAGE_CONTENT_MAX_SIZE-1);
      size_t msgsize = sizeof(PLMessageHeader) + msghdr->len;
      try {
	unique_ptr<char[]> msg(new char[msgsize]);
	memcpy(msg.get(), msgbuf, msgsize);
	message_pool[last_serial] =
	  PLTask{targetId, std::move(msg), msgsize, timeout_point(), 0};
	pending_messages_count++;
      } catch (std::bad_alloc &ba) {
	break;
      }
      pending_messages[targetId].insert(last_serial);
      last_serial++;
    }
  }
}

void sender_handle_acks() {
  ack_queue_lock.lock();

  while (!ack_queue.empty()) {
    uint64_t msgId = ack_queue.front();
    ack_queue.pop();
    auto ref = message_pool.find(msgId);
    if (ref != message_pool.end()) {
      message_pool.erase(ref);
      // cout << "PL ack " << msgId << endl;
      --pending_messages_count;
    }

    set<uint64_t> &targetPool =
        pending_messages[ref->second.receiverId];
    auto ref2 = targetPool.find(msgId);
    if (ref2 != targetPool.end()) {
      targetPool.erase(ref2);
    }
  }

  ack_queue_lock.unlock();
}

void sender_thread() {
  pending_messages_count = 0;
  last_serial = 0;
  sent_count = 0;
  ideal_pending_count = 40;

  while (true) {
    sender_handle_acks();
    sender_send_next();
    sender_retransmission();
    this_thread::sleep_for(20ms);
  }
}

void on_receive_data(PLPacketHeader *pkt) {
  static char logbuf[PL_MESSAGE_CONTENT_MAX_SIZE + 128];
  static char ackbuf[sizeof(PLPacketHeader) +
                     (sizeof(uint64_t) * PL_MESSAGE_GROUP_MAX_LENGTH)];
  PLMessageGroupHeader *grpHdr = reinterpret_cast<PLMessageGroupHeader *>(
      reinterpret_cast<char *>(pkt) + sizeof(PLPacketHeader));
  PLMessageHeader *msgHdr = PLFirstMsgHdr(grpHdr);
  unsigned long senderId = pkt->id;

  PLPacketHeader *ackpkt = reinterpret_cast<PLPacketHeader *>(ackbuf);
  uint64_t *curPkt =
      reinterpret_cast<uint64_t *>(ackbuf + sizeof(PLPacketHeader));
  size_t ackcnt = 0;
  ackpkt->id = myid;
  ackpkt->len = 0;
  ackpkt->type = ACK;

  for (size_t i = 0; i < grpHdr->numOfMsgs; i++) {
    if (delivered.count(PLMsgSig{senderId, msgHdr->messageId}) == 0) {
      // trigger deliver

      /*
      int offset = snprintf(logbuf, sizeof(logbuf), "d %lu ", senderId);
      offset += snprintf(logbuf + offset,
                         min(sizeof(logbuf) - static_cast<size_t>(offset) - 1,
                             min(msgHdr->len, PL_MESSAGE_CONTENT_MAX_SIZE)),
                         "%s", PLMsgBuf(msgHdr));
      logbuf[offset] = '\0';
      logger->logln(string(logbuf));
      */
      string msg_content(PLMsgBuf(msgHdr));
      // cout << "PerfLink Deliver " << senderId << " {" << msg_content << "}" << endl; 
      try {
	unique_ptr<Message> msg = msg_factory.parse(msg_content);
	// cout << "PerfLink Deliver: {\n" << msg->serialize() << "\n}" << endl;
	msg->accept(*fifo);
	// cout << "PerfLink Delivered" << endl;
      } catch(const invalid_argument &e) {
	cout << "receive error: " << e.what() << endl;
      }

      // cout << string(logbuf) << endl;
      delivered.insert(PLMsgSig{senderId, msgHdr->messageId});
    }
    if (ackcnt < PL_MESSAGE_GROUP_MAX_LENGTH) {
      curPkt[ackcnt] = msgHdr->messageId;
      ackpkt->len += sizeof(uint64_t);
      ++ackcnt;
    }
    msgHdr = PLNextMsgHdr(msgHdr);
  }
  udp->netsend(senderId, ackbuf, sizeof(PLPacketHeader) + ackpkt->len);
}

void on_receive_ack(PLPacketHeader *pkt) {
  size_t num_of_ack = pkt->len / sizeof(uint64_t);
  uint64_t *ackid = reinterpret_cast<uint64_t *>(reinterpret_cast<char *>(pkt) +
                                                 sizeof(PLPacketHeader));
  if (num_of_ack <= 0)
    return;

  ack_queue_lock.lock();
  for (size_t i = 0; i < num_of_ack; i++) {
    // cout << "PL recv ack " << ackid[i] << endl;
    ack_queue.push(ackid[i]);
  }
  ack_queue_lock.unlock();
}

void receiver_thread() {
  static char buffer[sizeof(PLPacketHeader) + sizeof(PLMessageGroupHeader) + PL_MESSAGE_GROUP_MAX_LENGTH*(sizeof(PLMessageHeader)+PL_MESSAGE_CONTENT_MAX_SIZE)+256];
  while (true) {
    receiver_broadcast_next_mymessage();
    size_t len = udp->netrecv(buffer, sizeof(buffer), 20ms);
    if (len > 0) {
      PLPacketHeader *header = reinterpret_cast<PLPacketHeader *>(buffer);
      char *msg = buffer + sizeof(PLPacketHeader);
      if (header->type == DATA) {
	on_receive_data(header);
      } else if (header->type == ACK) {
	on_receive_ack(header);
      } else {
	// error
      }
    } else {
      // cout << "netrecv timeout" << endl;
    }
    // broadcast new packet
  }
}


static uint64_t broadcast_count = 0;
void receiver_broadcast_next_mymessage() {
  if (broadcast_count < target_send_cnt) {
    // cout << "add " << broadcast_count << " to FIFObroadcast Queue" << endl;
    string content = to_string(++broadcast_count);
    fifo->broadcast(content);
  }
}
