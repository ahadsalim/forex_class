import MetaTrader5 as mt5

# Connect to MetaTrader 5
if not mt5.initialize():
    print("Failed to initialize MetaTrader 5")
    quit()

# Set the symbol you want to check, e.g., EURUSD
symbol = "USDNOK"

# Get symbol information
symbol_info = mt5.symbol_info(symbol)

if symbol_info is not None:
    # Get minimum stop level in points
    min_stop_level = symbol_info.trade_stops_level
    print(f"Minimum stop level for {symbol}: {min_stop_level} points")
else:
    print(f"Symbol {symbol} not found or not available.")

# Shut down MetaTrader 5
mt5.shutdown()
