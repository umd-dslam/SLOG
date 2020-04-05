#pragma once

#include <fstream>
#include <string>
#include <vector>

namespace slog {

const struct CSVWriterLineEnder{} csvendl;

class CSVWriter {
public:
  CSVWriter(
      const std::string& file_name,
      const std::vector<std::string>& columns,
      char delimiter=',');

  template<
    typename T,
    typename = std::enable_if_t<std::is_arithmetic<T>::value>>
  CSVWriter& operator<<(T val) {
    IncrementLineItemsAndCheck();
    AppendDelim();
    file_ << val;
    return *this;
  }

  CSVWriter& operator<<(const std::string& str);
  CSVWriter& operator<<(const CSVWriterLineEnder& ender);

private:
  void AppendDelim();
  void IncrementLineItemsAndCheck();

  size_t line_items_;
  size_t num_columns_;
  std::ofstream file_;
  char delim_;
};

} // namespace slog