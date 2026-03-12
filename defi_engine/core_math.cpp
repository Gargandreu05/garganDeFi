#include <pybind11/pybind11.h>
#include <cmath>
#include <stdexcept>

namespace py = pybind11;

// Namespace for DeFi mathematical operations
namespace math_engine {

    // Calculates Impermanent Loss given a price ratio k = P_new / P_old.
    // Returns a negative fraction (e.g. -0.057 means -5.7 %).
    double calculate_impermanent_loss(double price_ratio) {
        if (price_ratio <= 0.0) {
            throw std::invalid_argument("price_ratio must be positive");
        }
        double sqrt_k = std::sqrt(price_ratio);
        return (2.0 * sqrt_k / (1.0 + price_ratio)) - 1.0;
    }

    // Calculates the differential improvement between a candidate APY and current APY.
    // Returns the absolute difference.
    double calculate_apy_differential(double candidate_apy, double current_apy) {
        return candidate_apy - current_apy;
    }
}

// Pybind11 module definition
PYBIND11_MODULE(core_math, m) {
    m.doc() = "High-performance C++ math engine for GarganDeFi, bridged via pybind11";

    m.def("calculate_impermanent_loss", &math_engine::calculate_impermanent_loss, 
          "Calculates AMM Impermanent Loss given a price ratio (P_new / P_old)",
          py::arg("price_ratio"));

    m.def("calculate_apy_differential", &math_engine::calculate_apy_differential, 
          "Calculates the differential improvement between a candidate APY and current APY",
          py::arg("candidate_apy"), py::arg("current_apy"));
}
