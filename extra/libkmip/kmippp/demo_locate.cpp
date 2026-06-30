

#include "kmippp.h"
#include <iostream>

int
main (int argc, char **argv)
{

  if (argc < 7)
    {
      std::cerr << "Usage: demo_locate <host> <port> <client_cert> "
                   "<client_key> <server_cert> <key_name>"
                << std::endl;
      return -1;
    }

  kmippp::context ctx (argv[1], argv[2], argv[3], argv[4], argv[5]);

  auto keys = ctx.op_locate (argv[6]);
  if(keys.empty ())
    {
      std::cerr << "No Keys found" << std::endl;
      std::cerr << ctx.get_last_result () << std::endl;
      return 1;
    }

  for (auto id : keys)
    {
      std::cout << "Key: " << id << " 0x";
      auto key = ctx.op_get (id);
      for (auto const &c : key)
        {
          std::cout << std::hex << ((int)c);
        }
      std::cout << std::endl;
    }
  return 0;
}
