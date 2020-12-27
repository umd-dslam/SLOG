#include "common/csv_writer.h"

#include <stdexcept>

namespace slog {

CSVWriter::CSVWriter(const std::string& file_name, const std::vector<std::string>& columns, char delimiter) {
  if (columns.empty()) {
    throw std::runtime_error("There must be at least one column");
  }
  num_columns_ = columns.size();
  line_items_ = 0;
  delim_ = delimiter;

  file_ = std::ofstream(file_name, std::ios::out);
  if (!file_) {
    throw std::runtime_error(std::string("Cannot open file: ") + file_name);
  }
  bool first = true;
  for (const auto& col : columns) {
    if (!first) {
      file_ << ",";
    }
    file_ << col;
    first = false;
  }
  file_ << "\n";
}

void CSVWriter::AppendDelim() {
  if (line_items_ > 1) {
    file_ << delim_;
  }
}

void CSVWriter::IncrementLineItemsAndCheck() {
  line_items_++;
  if (line_items_ > num_columns_) {
    throw std::runtime_error("Number of items exceeds number of columns");
  }
}

}  // namespace slog