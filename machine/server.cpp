#include <iostream>
#include <google/protobuf/text_format.h>
#include <proto/config.pb.h>
#include <fstream>
#include <sstream>

using namespace slog;

int main() {
  // Verify that the version of the library that we linked against is
  // compatible with the version of the headers we compiled against
  GOOGLE_PROTOBUF_VERIFY_VERSION;
  
  std::ifstream f("slog.conf");
  std::stringstream ss;
  ss << f.rdbuf();
  Configuration config;
  google::protobuf::TextFormat::ParseFromString(ss.str(), &config);
  std::string str;
  google::protobuf::TextFormat::PrintToString(config, &str);
  printf("%s", str.c_str());
  return 0;
}