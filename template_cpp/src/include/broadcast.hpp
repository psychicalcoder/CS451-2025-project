#ifndef BROADCAST_H
#define BROADCAST_H

#include <cstdint>
#include <logger.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>

class Message;
class BroadcastMessage;
class BroadcastAckMessage;
struct BroadcastMessageSignature;

typedef std::chrono::time_point<std::chrono::high_resolution_clock> timeout_point;

class FIFOBroadcast {
private:
  uint64_t myId;
  // std::map<BroadcastMessageSignature, uint64_t> pending;
  std::unordered_map<uint64_t, uint64_t> next_deliver = {};
  // std::unordered_map<uint64_t, std::list<BroadcastMessageSignature>>
  //    can_deliver;

  std::map<BroadcastMessageSignature, std::unique_ptr<Message>> pool = {};

  Logger *logger;

  std::mutex pl_queue_lock = {};
  std::queue<std::unique_ptr<Message>> pl_queue[3] = {std::queue<std::unique_ptr<Message>>(), std::queue<std::unique_ptr<Message>>(), std::queue<std::unique_ptr<Message>>()}; // 0: broadcast queue, 1: relay queue, 2: heart beat queue;
  uint64_t pl_queue_rr_idx = 0;

  std::mutex ack_lock = {};
  std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint64_t>> other_acks = {};
  void update_my_ack(uint64_t src);
  std::vector<std::unique_ptr<BroadcastAckMessage>> my_ack_message();

  uint64_t next_serial = 1;
  uint64_t serial_number(bool ro = false) {
    uint64_t srl = next_serial;
    if (!ro) next_serial++;
    return srl;
  }

  timeout_point next_ack_time = timeout_point();

public:
  std::vector<uint64_t> hosts;

  FIFOBroadcast(uint64_t myId, std::vector<uint64_t> hosts, Logger *logger)
    : myId(myId), logger(logger), next_serial(1), hosts(hosts) {
    for (uint64_t id : hosts) {
      next_deliver[id] = 1;
      other_acks[id] = std::unordered_map<uint64_t, uint64_t>();
      for (uint64_t id2 : hosts) {
	other_acks[id][id2] = 0;
      }
      pl_queue_rr_idx = 0;
      // can_deliver[id] = std::list<BroadcastMessageSignature>();
    }
  }

  void handle(BroadcastMessage &);
  void handle(BroadcastAckMessage &);

  std::string next_message();

  void broadcast(std::string content);
};

class Message {
protected:
  std::string msgtype;

public:
  virtual std::string serialize() = 0;
  virtual void accept(FIFOBroadcast &handler) = 0;
  virtual void deliver(Logger* logger) = 0;
  virtual std::string type() { return msgtype; }
  virtual ~Message() = default;
};

const std::string ACKMSG = "BROADACK";
const std::string BRDMSG = "BROADMSG";

struct BroadcastMessageSignature {
  uint64_t source;
  uint64_t serial;
  BroadcastMessageSignature() {}
  BroadcastMessageSignature(uint64_t source, uint64_t serial)
      : source(source), serial(serial) {}
  BroadcastMessageSignature(const BroadcastMessageSignature &b)
      : source(b.source), serial(b.serial) {}
  bool operator<(const BroadcastMessageSignature &b) const {
    return this->source == b.source ? this->serial < b.serial
                                    : this->source < b.source;
  }
  bool operator==(const BroadcastMessageSignature &b) const {
    return this->source == b.source && this->serial == b.serial;
  }
  BroadcastMessageSignature &operator=(const BroadcastMessageSignature &b) {
    source = b.source;
    serial = b.serial;
    return *this;
  }
};

class BroadcastMessage : public Message {
public:
  BroadcastMessageSignature sig;
  std::string content;

  BroadcastMessage() { msgtype = BRDMSG; }
  BroadcastMessage(uint64_t source, uint64_t serial, std::string content)
      : sig(BroadcastMessageSignature(source, serial)), content(content) {
    msgtype = BRDMSG;
  }

  std::string serialize() override;
  void accept(FIFOBroadcast &handler) override { handler.handle(*this); }
  void deliver(Logger* logger) override;

  ~BroadcastMessage() = default;
};

class BroadcastAckMessage : public Message {
public:
  uint64_t source;
  std::vector<BroadcastMessageSignature> acks;
  BroadcastAckMessage() { msgtype = ACKMSG; }
  BroadcastAckMessage(uint64_t source) : source(source) { msgtype = ACKMSG; }
  ~BroadcastAckMessage() = default;

  std::string serialize() override;
  void accept(FIFOBroadcast &handler) override { handler.handle(*this); }
  void deliver(Logger*) override {}
};

class MessageFactory {
private:
  std::unique_ptr<BroadcastMessage> parse_broadcast(const std::string &raw,
                                                    size_t startpos);
  std::unique_ptr<BroadcastAckMessage> parse_ack(const std::string &raw,
                                                 size_t startpos);

public:
  std::unique_ptr<Message> parse(const std::string &raw);
};

#endif
