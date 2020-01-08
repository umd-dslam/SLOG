#include "module/scheduler_components/worker.h"

namespace slog {

Worker::Worker(zmq::context_t& context) : socket_(context, ZMQ_REP) {}

void Worker::SetUp() {

}

void Worker::Loop() {

}

} // namespace slog