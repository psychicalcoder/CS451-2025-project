#include <chrono>
#include <logger.hpp>
#include <mutex>
#include <parser.hpp>
#include <perflink.hpp>
#include <queue>
#include <set>
#include <thread>
#include <udp.hpp>
#include <iostream>

using namespace std;

struct pl_msghdr {
  unsigned long id;
  unsigned long long serial;
};

static bool operator<(const pl_msghdr &a, const pl_msghdr &b) {
  return a.id == b.id ? a.serial < b.serial : a.id < b.id;
}

mutex ack_queue_lock;
queue<uint64_t> ack_queue;

typedef chrono::time_point<chrono::high_resolution_clock> timeout_point;

extern UDP *udp;
extern Logger *logger;
extern unsigned long myid;

set<pl_msghdr> delivered;

struct pl_task {
  size_t receiverId;
  unsigned long long serial;
  size_t len;
  unique_ptr<char[]> pkt;
  timeout_point timeout;
  int retry;
};

vector<pl_task> tasks;
size_t active_tasks;
vector<bool> task_slot_busy;
int mx_retry;
unordered_map<size_t, size_t> serial2Task;

static void sender_handle_acks() {
  ack_queue_lock.lock();
  while (!ack_queue.empty()) {
    uint64_t serial = ack_queue.front();
    ack_queue.pop();
    auto ref = serial2Task.find(serial);
    if (ref == serial2Task.end())
      continue;
    size_t slotidx = ref->second;
    if (task_slot_busy[slotidx]) {
      task_slot_busy[slotidx] = false;
      active_tasks--;
    }
    serial2Task.erase(ref);
  }
  ack_queue_lock.unlock();
}

static void sender_retransimission() {
  timeout_point now = chrono::high_resolution_clock::now();
  size_t nTasks = tasks.size();
  for (size_t i = 0; i < nTasks; i++) {
    if (task_slot_busy[i] && tasks[i].timeout < now) {
      pl_task &t = tasks[i];
      udp->netsend(t.receiverId, t.pkt.get(), t.len);
      t.timeout = now + 200ms;
      t.retry++;
      // cout << "send to " << t.receiverId << endl;
    }
    // TODO stop retry after 16 times
  }
}

extern unsigned long target_send_cnt;
extern unsigned long target_id;

unsigned long sent_cnt;
unsigned long long currentSerial;

static void sender_send_next() {
  static char msgbuf[4096];
  size_t idx = 0;

  size_t nSlots = tasks.size();
  if (active_tasks == tasks.size()) {
    nSlots++;
    tasks.resize(nSlots);
    task_slot_busy.resize(nSlots);
    idx = nSlots - 1;
  }
  timeout_point outdated = chrono::high_resolution_clock::now();
  while (active_tasks < nSlots && sent_cnt < target_send_cnt) {
    sent_cnt++;
    snprintf(msgbuf, sizeof(msgbuf), "b %lu", sent_cnt);
    logger->logln(string(msgbuf));
    int len = snprintf(msgbuf, sizeof(msgbuf), "%lu", sent_cnt);
    size_t datasz = static_cast<size_t>(len) + 1;
    size_t pktsz = sizeof(PLPacketHeader) + datasz;
    unique_ptr<char[]> pkt(new char[pktsz]);
    strncpy(pkt.get() + sizeof(PLPacketHeader), msgbuf, datasz);
    PLPacketHeader *hdr = reinterpret_cast<PLPacketHeader *>(pkt.get());
    hdr->id = myid;
    hdr->len = datasz;
    hdr->serial = ++currentSerial;
    hdr->type = DATA;

    while (idx < nSlots) {
      if (!task_slot_busy[idx]) {
        tasks[idx] = pl_task{target_id,
                             hdr->serial,
                             pktsz,
                             std::move(pkt),
                             chrono::high_resolution_clock::now(),
                             0};
        task_slot_busy[idx] = true;
	serial2Task[hdr->serial] = idx;
        idx++;
        break;
      } else {
        idx++;
      }
    }
    active_tasks++;
  }
}

void sender_thread() {
  tasks.resize(20);
  task_slot_busy.resize(20);
  active_tasks = 0;

  while (true) {
    sender_handle_acks();
    sender_send_next();
    sender_retransimission();
    this_thread::sleep_for(50ms);
  }
}

void receiver_thread() {
  static char buffer[4096];
  static char logbuf[4096];
  while (true) {
    size_t len = udp->netrecv(buffer, sizeof(buffer));
    PLPacketHeader *header = reinterpret_cast<PLPacketHeader *>(buffer);
    char *msg = buffer + sizeof(PLPacketHeader);

    // cout << "recv from " << header->id << endl;
    
    if (header->type == DATA) {
      if (delivered.count(*(reinterpret_cast<pl_msghdr *>(header))) == 0) {
        // new message
        // trigger deliver
        msg[len] = '\0';
        snprintf(logbuf, sizeof(logbuf), "d %lu %s", header->id, msg);
	logger->logln(string(logbuf));
        delivered.insert(pl_msghdr{header->id, header->serial});
      }

      // send ack back
      PLPacketHeader ackpkt{myid, header->serial, 0, ACK};
      udp->netsend(header->id, reinterpret_cast<char *>(&ackpkt),
		   sizeof(ackpkt));
    } else if (header->type == ACK) {
      ack_queue_lock.lock();
      ack_queue.push(header->serial);
      ack_queue_lock.unlock();
    } else {
      // error
    }
  }
}
