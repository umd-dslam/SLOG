#include "benchmark/stored_procedures.h"

using std::string;
using std::vector;

namespace slog {

namespace {

const string SPACE(" \t\n\v\f\r");

size_t NextToken(string& token, const string& str, size_t pos) {
  while (pos < str.length() && isspace(str[pos])) {
    pos++;
  }
  if (pos >= str.length()) {
    return string::npos;
  }
  auto length = str.find_first_of(SPACE, pos) - pos;
  token = str.substr(pos, length);
  return pos + token.length();
}

size_t NextNTokens(
    vector<string>& tokens, const string& str, size_t pos, size_t n = 1) {
  tokens.clear();
  for (size_t i = 0; i < n; i++) {
    string token;
    pos = NextToken(token, str, pos);
    if (pos == string::npos) {
      return string::npos;
    }
    tokens.push_back(std::move(token));
  }
  return pos;
}

} // namespace

void KeyValueStoredProcedures::Execute(Transaction& txn) {
  auto& write_set = *txn.mutable_write_set();
  auto& delete_set = *txn.mutable_delete_set();
  string cmd;
  vector<string> args;
  size_t pos = 0;

  txn.set_status(TransactionStatus::COMMITTED);

  while ((pos = NextToken(cmd, txn.code(), pos))
      != string::npos) {
    if (cmd == "SET") {
      pos = NextNTokens(args, txn.code(), pos, 2);
      if (pos == string::npos) {
        txn.set_status(TransactionStatus::ABORTED);
        txn.set_abort_reason("Invalid transaction code");
        break;
      }
      if (write_set.contains(args[0])) {
        write_set[args[0]] = std::move(args[1]);
      } 
    } else if (cmd == "DEL") {
      pos = NextNTokens(args, txn.code(), pos);
      if (pos == string::npos) {
        txn.set_status(TransactionStatus::ABORTED);
        txn.set_abort_reason("Invalid transaction code");
        break;
      }
      if (write_set.contains(args[0])) {
        delete_set.Add(std::move(args[0]));
      }
    }
  }
}

void TPCCStoredProcedures::Execute(Transaction& txn) {
  
}

} // namespace slog