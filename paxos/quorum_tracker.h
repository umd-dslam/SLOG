#pragma once

#include <unordered_set>
#include <vector>

#include "common/types.h"
#include "proto/internal.pb.h"

using std::string;
using std::unordered_set;
using std::vector;

namespace slog {

enum class QuorumState {
  INCOMPLETE,
  QUORUM_REACHED,
  COMPLETE,
  ABORTED
};

class QuorumTracker {
public:
  QuorumTracker(uint32_t num_members);
  virtual ~QuorumTracker() = default;

  bool HandleResponse(const internal::Response& res, MachineId from);
    
  QuorumState GetState() const;
 
protected:
  virtual bool ResponseIsValid(const internal::Response& res) = 0;
  
  void Abort();

private:
  uint32_t num_members_;
  unordered_set<MachineId> machine_responded_;
  QuorumState state_;
};

class AcceptanceTracker : public QuorumTracker {
public:
  AcceptanceTracker(
      uint32_t num_members,
      uint32_t ballot,
      uint32_t slot);

  const uint32_t ballot;
  const uint32_t slot;

protected:
  bool ResponseIsValid(const internal::Response& res) final;
};

class CommitTracker : public QuorumTracker {
public:
  CommitTracker(
      uint32_t num_members,
      uint32_t slot);
  
  const uint32_t slot;

protected:
  bool ResponseIsValid(const internal::Response& res) final;
};

} // namespace slog