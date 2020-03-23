#pragma once

#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "proto/internal.pb.h"

using std::string;
using std::unordered_set;
using std::unordered_map;
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

  bool HandleResponse(
      const internal::Response& res,
      const string& from_machine_id);
    
  QuorumState GetState() const;
 
protected:
  virtual bool ResponseIsValid(const internal::Response& res) = 0;
  
  void Abort();

private:
  uint32_t num_members_;
  unordered_set<string> machine_responded_;
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