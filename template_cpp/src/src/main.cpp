#include <chrono>
#include <iostream>
#include <thread>

#include "parser.hpp"
#include "perflink.hpp"
#include <logger.hpp>
#include <signal.h>
#include <udp.hpp>
#include <unordered_map>

UDP *udp;
Logger *logger;
unsigned long myid;
unsigned long target_send_cnt;
unsigned long target_id;

static void stop(int) {
  // reset signal handlers to default
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);

  // immediately stop network packet processing
  std::cout << "Immediately stopping network packet processing.\n";

  delete udp;
  delete logger;

  // write/flush output file if necessary
  // std::cout << "Writing output.\n";

  // exit directly from signal handler
  exit(0);
}

static void prepare(Parser &parser) {
  myid = parser.id();
  auto hosts = parser.hosts();

  std::unordered_map<uint64_t, uint64_t> id2Addr;
  vector<sockaddr_in> addrs(hosts.size());
  for (size_t i = 0; i < hosts.size(); i++) {
    auto &host = hosts[i];
    addrs[i].sin_addr.s_addr = host.ip;
    addrs[i].sin_port = host.port;
    addrs[i].sin_family = AF_INET;
    id2Addr[host.id] = i;
  }
  logger = new Logger(string(parser.outputPath()));
  udp = new UDP(myid, addrs, id2Addr);
}

int main(int argc, char **argv) {
  signal(SIGTERM, stop);
  signal(SIGINT, stop);

  // `true` means that a config file is required.
  // Call with `false` if no config file is necessary.
  bool requireConfig = true;

  Parser parser(argc, argv);
  parser.parse();
  
  std::cout << std::endl;

  std::cout << "My PID: " << getpid() << "\n";
  std::cout << "From a new terminal type `kill -SIGINT " << getpid()
            << "` or `kill -SIGTERM " << getpid()
            << "` to stop processing packets\n\n";

  std::cout << "My ID: " << parser.id() << "\n\n";

  std::cout << "List of resolved hosts is:\n";
  std::cout << "==========================\n";
  auto hosts = parser.hosts();
  for (auto &host : hosts) {
    std::cout << host.id << "\n";
    std::cout << "Human-readable IP: " << host.ipReadable() << "\n";
    std::cout << "Machine-readable IP: " << host.ip << "\n";
    std::cout << "Human-readbale Port: " << host.portReadable() << "\n";
    std::cout << "Machine-readbale Port: " << host.port << "\n";
    std::cout << "\n";
  }
  std::cout << "\n";

  std::cout << "Path to output:\n";
  std::cout << "===============\n";
  std::cout << parser.outputPath() << "\n\n";

  std::cout << "Path to config:\n";
  std::cout << "===============\n";
  std::cout << parser.configPath() << "\n\n";

  std::cout << "Doing some initialization...\n\n";

  std::cout << "Broadcasting and delivering messages...\n\n";

  PerfConfig cfg(parser.configPath());

  target_id = cfg.receiverId;
  target_send_cnt = cfg.packetCount;

  prepare(parser);
  if (parser.id() == cfg.receiverId) { 
    std::cout << "I am the receiver\n\n";
    target_send_cnt = 0;
    auto t1 = std::thread(receiver_thread);
    auto t2 = std::thread(sender_thread);
    t1.join();
    t2.join();
  } else {
    std::cout << "I am " << myid << " send to " << target_id << "\n\n";
    auto t1 = std::thread(receiver_thread);
    auto t2 = std::thread(sender_thread);
    t1.join();
    t2.join();
  }

  // After a process finishes broadcasting,
  // it waits forever for the delivery of messages.
  while (true) {
    std::this_thread::sleep_for(std::chrono::hours(1));
  }

  return 0;
}
