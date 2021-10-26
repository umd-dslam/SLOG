#pragma once

#include <vector>

#include "common/configuration.h"
#include "common/types.h"
#include "proto/transaction.pb.h"
#include "workload/workload.h"

namespace slog {

class CockroachWorkload : public Workload {
 public:
  class ZipfDistribution {
    public:
      ZipfDistribution(int a, int b, double theta) : a_(a), b_(b), theta_(theta) {
        zeta2_ = ComputeZeta(2);
        zetaN_ = ComputeZeta(b_-a_+1);
        alpha_ = 1.0 / (1.0 - theta_);
        eta_ = (1 - std::pow(2.0/(b_-a_+1), 1.0 - theta_)) / (1.0 - zeta2_/zetaN_);
        half_pow_theta_ = 1.0 + std::pow(0.5, theta_);
      }

      template<class Generator>
      double operator()(Generator& g) {
        std::uniform_real_distribution<> d;
        double u = d(g);
        double uz = u * zetaN_;
        if (uz < 1) {
          return a_;
        }    
        if (uz < half_pow_theta_) {
          return a_ + 1;
        }
        double spread = b_ - a_ + 1;
        return a_ + spread * std::pow(eta_*u - eta_ + 1.0, alpha_);
      }

      int Max() const {
        return b_;
      }

    private:
      double ComputeZeta(int n) {
        double res = 0;
        for (int i = 1; i <= n; i++) {
          res += 1.0 / std::pow(i, theta_);
        }
        return res;
      }

      int a_;
      int b_;
      double theta_;
      double zeta2_;
      double zetaN_;
      double alpha_;
      double eta_;
      double half_pow_theta_;
  };

  CockroachWorkload(const ConfigurationPtr& config, uint32_t region, const std::string& params_str,
                    const uint32_t seed = std::random_device()());

  std::pair<Transaction*, TransactionProfile> NextTransaction();

 private:
  ConfigurationPtr config_;
  uint32_t local_region_;
  ZipfDistribution zipf_;

  std::mt19937 rg_;
  RandomStringGenerator rnd_str_;

  TxnId client_txn_id_counter_;

  std::vector<uint64_t> KeyBatch(int n);
};

}  // namespace slog