

#include "kmippp.h"
#include <cstring>
#include <iostream>

int
main (int argc, char **argv)
{

  if (argc < 7)
    {
      std::cerr << "Usage: demo_activate <host> <port> <client_cert> "
                   "<client_key> <server_cert> <key_id>"
                << std::endl;
      return -1;
    }

  kmippp::context ctx (argv[1], argv[2], argv[3], argv[4], argv[5]);
  std::string     key_id = argv[6];

  if (!ctx.op_activate (key_id))
    {
      std::cerr << "Failed to activate key " << key_id << std::endl;
      std::cout << ctx.get_last_result () << std::endl;
      return -1;
    }

  std::cout << "Key: " << key_id << " activated." << std::endl;
  std::cout << "end!" << std::endl;

  return 0;
}
