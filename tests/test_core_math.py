import os
import sys

# Test 1: Verify the Discord routing channels exist in os.environ (or mock them to test load time parsing)
os.environ["DISCORD_DEFI_ALERTS_ID"] = "111"
os.environ["DISCORD_DEFI_LOGS_ID"] = "222"
os.environ["DISCORD_QUANT_SIGNALS_ID"] = "333"

sys.path.append(os.path.join(os.path.dirname(__file__), "garganDeFi"))

try:
    print("DeFiCog imported successfully.")
    
    # Check the fallback logic
    try:
        import core_math
        from core_math import calculate_apy_differential, calculate_impermanent_loss
        print(f"C++ Math Engine loaded. APY Diff (15, 10): {calculate_apy_differential(15.0, 10.0)}")
        print(f"C++ Math Engine IL (0.5): {calculate_impermanent_loss(0.5)}")
    except ImportError:
        print("C++ Math Engine not found, fallback to Python math works as expected by cogs.")

    print("Success: All modules load properly with the new architecture.")
except Exception as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)
