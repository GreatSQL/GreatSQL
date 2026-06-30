

#include "kmippp.h"
#include <iostream>

int
main (int argc, char **argv)
{

  if (argc < 7)
    {
      std::cerr << "Usage: demo_create <host> <port> <client_cert> "
                   "<client_key> <server_cert> <key_id>"
                << std::endl;
      return -1;
    }

  kmippp::context ctx (argv[1], argv[2], argv[3], argv[4], argv[5]);

  auto key = ctx.op_get_name_attr (argv[6]);
  if(key.empty ())
    {
      std::cout << ctx.get_last_result () << std::endl;
      return 1;
    }
  std::cout << "Name: " << key;
  std::cout << std::endl;
  return 0;
}
