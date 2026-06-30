

#include "kmippp.h"
#include <cstring>
#include <iostream>

int
main (int argc, char **argv)
{

  if (argc < 8)
    {
      std::cerr << "Usage: demo_register_secret <host> <port> <client_cert> "
                   "<client_key> <server_cert> <secret_name> "
                   "<secrtet>"
                << std::endl;
      return -1;
    }

  kmippp::context ctx (argv[1], argv[2], argv[3], argv[4], argv[5]);

  kmippp::context::secret_t secret (argv[7]);
  // secret types: password: 1, seed: 2
  std::string               secret_id = ctx.op_register_secret (argv[6], "TestGroup", secret, 1);
  std::cout << "New secret: " << secret_id << std::endl;

  std::cout << "end!" << std::endl;

  return 0;
}
