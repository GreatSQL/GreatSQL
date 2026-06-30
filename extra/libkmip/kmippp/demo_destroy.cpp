

#include "kmippp.h"
#include <iostream>

int
main (int argc, char **argv)
{

  if (argc < 7)
    {
      std::cerr << "Usage: demo_destroy <host> <port> <client_cert> "
                   "<client_key> <server_cert> <key_id>"
                << std::endl;
      return -1;
    }

  kmippp::context ctx (argv[1], argv[2], argv[3], argv[4], argv[5]);

  auto res = ctx.op_destroy (argv[6]);
  if (res)
    {
      std::cout << "Key: " << argv[6] << " deleted" << std::endl;
      return 0;
    }
  std::cerr << "Key: " << argv[6] << " is not deleted" << std::endl;
  std::cout << ctx.get_last_result () << std::endl;
  std::cout << std::endl;
  return 1;
}
