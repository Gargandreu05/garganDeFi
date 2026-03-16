#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <cmath>

namespace py = pybind11;

float calculate_impermanent_loss(float price_ratio) {
    // Formula: (2 * sqrt(price_ratio)) / (1 + price_ratio) - 1
    if (price_ratio <= 0) return 0.0f;
    return (2.0f * std::sqrt(price_ratio)) / (1.0f + price_ratio) - 1.0f;
}

float calculate_apy_differential(float apy_a, float apy_b) {
    return apy_a - apy_b;
}

float calculate_net_apy(float gross_apy, float il_pct, float fee_rate) {
    // Basic net APY: gross - IL - fees
    return gross_apy - std::abs(il_pct) - fee_rate;
}

py::dict compute_zap_amounts(float total_sol, float gas_reserve) {
    float deployable = total_sol - gas_reserve;
    float sol_to_swap = deployable > 0 ? deployable / 2.0f : 0.0f;
    float sol_for_base = deployable > 0 ? deployable / 2.0f : 0.0f;

    py::dict d;
    d["total_sol"] = total_sol;
    d["gas_reserve"] = gas_reserve;
    d["deployable"] = deployable;
    d["sol_to_swap"] = sol_to_swap;
    d["sol_for_base"] = sol_for_base;
    return d;
}

PYBIND11_MODULE(core_math, m) {
    m.doc() = "GarganDeFi C++ Math Engine";
    m.def("calculate_impermanent_loss", &calculate_impermanent_loss, "Calculate IL");
    m.def("calculate_apy_differential", &calculate_apy_differential, "Calculate APY Diff");
    m.def("calculate_net_apy", &calculate_net_apy, "Calculate Net APY");
    m.def("compute_zap_amounts", &compute_zap_amounts, "Compute Zap Amounts");
}
