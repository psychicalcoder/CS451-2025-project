#ifndef CS451_UDP_H
#define CS451_UDP_H

#include <arpa/inet.h>
#include <cstring>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <fcntl.h>

class UDP {
private:
  int sockfd;
  sockaddr_in servaddr;

  std::vector<sockaddr_in> hosts;
  std::unordered_map<size_t, size_t> id2Addr;

public:
  UDP(uint64_t myid, std::vector<sockaddr_in> hosts,
      std::unordered_map<size_t, size_t> id2Addr)
      : hosts(hosts), id2Addr(id2Addr) {
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
      throw std::runtime_error("socket(): " + std::string(strerror(errno)));
    }
    sockaddr_in *servaddr = &hosts[id2Addr[myid]];
    int err = bind(sockfd, reinterpret_cast<const struct sockaddr *>(servaddr),
                   sizeof(sockaddr_in));
    if (err < 0) {
      throw std::runtime_error("bind(): " + std::string(strerror(errno)));
    }
  }

  int netsend(size_t targetID, char *buf, size_t len);
  size_t netrecv(char *buf, size_t maxlen);

  ~UDP() { close(sockfd); }
};

#endif
