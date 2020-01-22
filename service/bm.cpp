#include "common/service_utils.h"
#include "benchmark/workload_generator.h"

DEFINE_string(host, "localhost", "Hostname of the SLOG server to connect to");
DEFINE_uint32(port, 5051, "Port number of the SLOG server to connect to");

using namespace slog;

int main(int argc, char* argv[]) {
  InitializeService(argc, argv);

  auto context = std::make_shared<zmq::context_t>(1);
  auto generator = MakeRunnerFor<WorkloadGenerator>(context, FLAGS_host, FLAGS_port);

  generator->Start();
 
  return 0;
}