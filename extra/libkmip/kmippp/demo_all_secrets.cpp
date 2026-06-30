

#include "kmippp.h"
#include <iostream>

int
main (int argc, char **argv)
{

  if (argc < 6)
    {
      std::cerr << "Usage: demo_locate <host> <port> <client_cert> "
                   "<client_key> <server_cert> [group_name]"
                << std::endl;
      return -1;
    }

  kmippp::context ctx (argv[1], argv[2], argv[3], argv[4], argv[5]);
  // auto keys = ctx.op_all_secrets();
   const std::string group = argv[6]!=nullptr? argv[6] : "TestGroup";
   auto keys = ctx.op_locate_secrets_by_group (group);
  if(keys.empty ())
    {
      std::cerr << "No Secret Data  found" << std::endl;
      std::cerr << ctx.get_last_result () << std::endl;
      return 1;
    }
  for (auto id : keys)
    {
      std::cout << "Key: " << id << " ";
      auto secret      = ctx.op_get_secret (id);
      auto secret_name = ctx.op_get_name_attr (id);
      std::cout << secret_name << " 0x";
      for (auto const &c : secret)
        {
          std::cout << std::hex << ((int)c);
        }
      std::cout << std::endl;
    }
  return 0;
}
