

#include "kmippp.h"
#include <iostream>

int
main (int argc, char **argv)
{

  if (argc < 7)
    {
      std::cerr << "Usage: demo_get_secret <host> <port> <client_cert> "
                   "<client_key> <server_cert> <secret_id>"
                << std::endl;
      return -1;
    }

  kmippp::context ctx (argv[1], argv[2], argv[3], argv[4], argv[5]);

  auto secret = ctx.op_get_secret (argv[6]);
  if(secret.empty ())
    {
      std::cout << ctx.get_last_result () << std::endl;
      return 1;
    }
  std::cout << "Secret: " << secret << std::endl;
  return 0;
}
