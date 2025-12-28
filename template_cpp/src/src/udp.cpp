#include <sys/time.h>
#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <udp.hpp>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <iostream>

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

using namespace std::chrono;

size_t UDP::netrecv(char *buf, size_t maxlen, std::chrono::milliseconds timeout) {
  sockaddr_in sender;
  memset(&sender, 0, sizeof(sender));

  auto secs = duration_cast<seconds>(timeout);
  auto usecs = duration_cast<microseconds>(timeout - secs);
  timeval ctimeout;
  ctimeout.tv_sec = secs.count();
  ctimeout.tv_usec = usecs.count();

  int retval;
  fd_set rfds;
  
  FD_ZERO(&rfds);
  FD_SET(sockfd, &rfds);
  

  retval = select(sockfd+1, &rfds, NULL, NULL, &ctimeout);
  // retval = select(sockfd+1, &rfds, NULL, NULL, NULL);
  if (retval < 0) {
    throw std::runtime_error(std::string(strerror(errno)));
  } else if (retval == 0) {
    return 0;
  }

  
  ssize_t ret = recv(sockfd, buf, maxlen, 0);
  if (ret < 0) {
    throw std::runtime_error(std::string(strerror(errno)));
  }
  return static_cast<size_t>(ret);
}
