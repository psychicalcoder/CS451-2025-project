#include <cerrno>
#include <stdexcept>
#include <udp.hpp>
#include <sys/socket.h>
#include <arpa/inet.h>

int UDP::netsend(size_t targetID, char *buf, size_t len) {
  auto ref = id2Addr.find(targetID);
  if (ref == id2Addr.end()) {
    return -1;
  }
  
  size_t idx = ref->second;
  sockaddr_in* addr = &hosts[idx];
  ssize_t ret = sendto(sockfd, buf, len, MSG_DONTWAIT, reinterpret_cast<const sockaddr*>(addr), sizeof(sockaddr_in));
  if (ret < 0) {
    if (errno == EWOULDBLOCK) return -EWOULDBLOCK;
    else if (errno == ENOBUFS) return -ENOBUFS;
    else 
      throw std::runtime_error(std::string(strerror(errno)));
  }
  return 0;
}

size_t UDP::netrecv(char *buf, size_t maxlen) {
  sockaddr_in sender;
  memset(&sender, 0, sizeof(sender));
  ssize_t ret = recv(sockfd, buf, maxlen, 0);
  if (ret < 0) {
    throw std::runtime_error(std::string(strerror(errno)));
  }
  return static_cast<size_t>(ret);
}
