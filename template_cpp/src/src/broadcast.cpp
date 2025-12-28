#include <broadcast.hpp>
#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace std;

string BroadcastMessage::serialize() {
  stringstream ss;
  ss << msgtype << '\n' << sig.source << '\n' << sig.serial << '\n' << content;
  return ss.str();
}

uint64_t ack_sent_cnt = 0;

string BroadcastAckMessage::serialize() {
  stringstream ss;
  ss << msgtype << '\n' << source;
  for (const BroadcastMessageSignature &sig : acks) {
    ss << '\n' << sig.source << '\n' << sig.serial;
  }
  ack_sent_cnt++;
  return ss.str();
}

static size_t find_newline(const std::string &raw, size_t startpos) {
  size_t it = 0;
  size_t len = raw.length();
  for (it = startpos; it < len; it++) {
    if (raw[it] == '\n') {
      break;
    }
  }
  return it;
}

unique_ptr<Message> MessageFactory::parse(const string &raw) {
  size_t it = find_newline(raw, 0);
  string type = raw.substr(0, it);
  unique_ptr<Message> ret;
  if (type == BRDMSG) {
    return parse_broadcast(raw, it + 1);
  } else if (type == ACKMSG) {
    return parse_ack(raw, it + 1);
  } else {
    // stringstream ss;
    // ss << "unknown type: " << '"' << type << "\", should be " << BRDMSG << "
    // or " << ACKMSG;
    throw invalid_argument("unknown type: \"" + type + "\"");
    // throw invalid_argument(ss.str());
  }
}

unique_ptr<BroadcastMessage>
MessageFactory::parse_broadcast(const std::string &raw, size_t startpos) {
  size_t len = raw.length();
  size_t id_end = find_newline(raw, startpos);
  size_t serial_start = id_end + 1;
  size_t serial_end = find_newline(raw, serial_start);
  string str_id = raw.substr(startpos, id_end - startpos);
  string str_serial = raw.substr(id_end + 1, serial_end - (serial_start));
  string content = raw.substr(serial_end + 1);

  uint64_t id = stoul(str_id);
  uint64_t serial = stoul(str_serial);

  return make_unique<BroadcastMessage>(id, serial, content);
}

unique_ptr<BroadcastAckMessage> MessageFactory::parse_ack(const string &raw,
                                                          size_t startpos) {
  size_t len = raw.length();
  size_t id_end = find_newline(raw, startpos);
  string str_id = raw.substr(startpos, id_end - startpos);
  uint64_t id = stoul(str_id);

  size_t nnl = id_end, start = id_end + 1;
  unique_ptr<BroadcastAckMessage> msg = make_unique<BroadcastAckMessage>(id);

  int round_serial = 0;
  string cache[2];
  while (start < len) {
    nnl = find_newline(raw, start);
    cache[round_serial] = raw.substr(start, nnl - start);

    if (round_serial) {
      uint64_t src = stoul(cache[0]);
      uint64_t srl = stoul(cache[1]);
      msg->acks.push_back(BroadcastMessageSignature(src, srl));
    }

    round_serial ^= 1;
    start = nnl + 1;
  }
  return msg;
}

void FIFOBroadcast::update_my_ack(uint64_t src) {
  // update myack map, ack means the one have all the message before that serial
  // cout << "update_my_ack(" << src << ")\n";
  lock_guard<mutex> guard(ack_lock);
  uint64_t next_ack_srl = 0;
  {
    auto it = other_acks[myId].find(src);
    if (it != other_acks[myId].end())
      next_ack_srl = it->second + 1;
  }
  bool new_ack = false;
  while (true) {
    // cout << "  checking " << next_ack_srl << endl;
    auto it = pool.find(BroadcastMessageSignature(src, next_ack_srl));
    if (it == pool.end())
      break;
    new_ack = true;
    next_ack_srl++;
  }
  if (new_ack) {
    other_acks[myId][src] = next_ack_srl - 1;
  }
}

void FIFOBroadcast::handle(BroadcastMessage &msg) {
  auto it = pool.find(msg.sig);
  if (it != pool.end()) {
    return;
  }

  // cout << "recv broadcast " << msg.sig.source << ' ' << msg.content << endl;
  // cout << "r " << msg.sig.source << ' ' << msg.content << endl;

  pool[msg.sig] = make_unique<BroadcastMessage>(msg.sig.source, msg.sig.serial,
                                                msg.content);

  update_my_ack(msg.sig.source);

  {
    lock_guard<mutex> guard(pl_queue_lock);
    // if it is the first time see this message, relay it.
    pl_queue[1].push(make_unique<BroadcastMessage>(
        msg.sig.source, msg.sig.serial, msg.content));
  }
}

// this is called by the receiver thread
void FIFOBroadcast::handle(BroadcastAckMessage &acks) {
  // cout << "FIFO: handle ack from " << acks.source << endl;
  lock_guard<mutex> guard(ack_lock);
  // cout << "ack_lock acquired" << endl;

  uint64_t acker = acks.source;
  auto &ackmap = other_acks[acker];

  // upder other acks
  for (const BroadcastMessageSignature &sig : acks.acks) {
    // cout << "  " << sig.source << " " << sig.serial << endl;
    auto it = ackmap.find(sig.source);
    if (it == ackmap.end()) {
      ackmap[sig.source] = sig.serial;
    } else {
      if (it->second < sig.serial)
        it->second = sig.serial;
    }
  }

  // cout << "checking deliverable..." << endl;
  for (auto &next_del : next_deliver) {
    bool new_deliver_p = false;
    uint64_t senderId = next_del.first;
    // cout << "checking " << next_del.first << endl;
    do {
      uint64_t next_dsrl = next_del.second;
      new_deliver_p = false;

      // cout << "  checking (" << senderId << "," << next_dsrl << ")" << endl;

      auto be_del_msg_it =
          pool.find(BroadcastMessageSignature(senderId, next_dsrl));
      if (be_del_msg_it == pool.end())
        continue;

      uint64_t n_acker = 0;
      for (auto &oacker : other_acks) {
        if (oacker.second[senderId] >= next_dsrl)
          n_acker++;
      }
      // cout << "    = " << n_acker << endl;

      if (n_acker > (hosts.size() >> 1)) {
        // cout << "can deliver " << senderId << " " << next_dsrl << endl;
        next_del.second++;
        be_del_msg_it->second->deliver(logger);
        pool.erase(be_del_msg_it);
        new_deliver_p = true;
      }
    } while (new_deliver_p);
  }
}

void BroadcastMessage::deliver(Logger *logger) {
  stringstream ss;
  ss << "d " << sig.source << ' ' << content;
  // cout << ss.str() << endl;
  logger->logln(ss.str());
}

string FIFOBroadcast::next_message() {
  string raw = "";
  BroadcastMessageSignature sig;

  timeout_point now = chrono::high_resolution_clock::now();

  {
    lock_guard<mutex> guard(pl_queue_lock);
    if (next_ack_time < now) {
      next_ack_time = now + 500ms;
      auto ackmsgs = my_ack_message();
      auto it = ackmsgs.begin();
      while (it != ackmsgs.end()) {
	pl_queue[2].push(std::move((*it)));
	it++;
      }
    }
    for (uint64_t i = 0; i < 3; i++) {
      if (!pl_queue[pl_queue_rr_idx].empty()) {
        auto msgptr = std::move(pl_queue[pl_queue_rr_idx].front());
        pl_queue[pl_queue_rr_idx].pop();
        raw = msgptr->serialize();
        pl_queue_rr_idx = (pl_queue_rr_idx + 1) % 3;
        break;
      }
      pl_queue_rr_idx = (pl_queue_rr_idx + 1) % 3;
    }
  }
  return raw;
}

void FIFOBroadcast::broadcast(string content) {
  uint64_t srl = this->serial_number();
  pool[BroadcastMessageSignature(myId, srl)] =
      make_unique<BroadcastMessage>(BroadcastMessage(myId, srl, content));

  update_my_ack(myId);

  {
    lock_guard<mutex> guard(pl_queue_lock);
    pl_queue[0].push(make_unique<BroadcastMessage>(myId, srl, content));
  }

  // cout << "b " << content << endl;
  logger->logln("b " + content);
}

vector<unique_ptr<BroadcastAckMessage>> FIFOBroadcast::my_ack_message() {
  lock_guard<mutex> guard(ack_lock);
  vector<unique_ptr<BroadcastAckMessage>> ackmsgs;
  uint64_t curcnt = 0;
  ackmsgs.push_back(make_unique<BroadcastAckMessage>(myId));
  // cout << "myack ";
  for (auto &other : other_acks[myId]) {
    if (ackmsgs.back()->acks.size() == 40)
      ackmsgs.push_back(make_unique<BroadcastAckMessage>(myId));
    ackmsgs.back()->acks.push_back(BroadcastMessageSignature(other.first, other.second));
    // cout << other.first << "/" << other.second << " ";
  }
  // cout << endl;
  return ackmsgs;
}
