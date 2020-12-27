#pragma once

#include <fstream>
#include <string>
#include <vector>

namespace slog {

const struct CSVWriterLineEnder {
} csvendl;

class CSVWriter {
 public:
  CSVWriter(const std::string& file_name, const std::vector<std::string>& columns, char delimiter = ',');

  template <typename T, typename = std::enable_if_t<std::is_arithmetic<T>::value>>
  CSVWriter& operator<<(T val) {
    IncrementLineItemsAndCheck();
    AppendDelim();
    file_ << val;
    return *this;
  }

  CSVWriter& operator<<(const std::string& str) {
    IncrementLineItemsAndCheck();
    AppendDelim();
    file_ << str;
    return *this;
  }

  CSVWriter& operator<<(const CSVWriterLineEnder& ender) {
    if (line_items_ != num_columns_) {
      throw std::runtime_error("Number of items must match number of columns");
    }
    (void)ender;  // Silent unused warning
    file_ << "\n";
    line_items_ = 0;
    return *this;
  }

 private:
  void AppendDelim();
  void IncrementLineItemsAndCheck();

  size_t line_items_;
  size_t num_columns_;
  std::ofstream file_;
  char delim_;
};

}  // namespace slog