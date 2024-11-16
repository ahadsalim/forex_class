import MetaTrader5 as mt5

# Initialize MetaTrader 5 connection
if not mt5.initialize():
    print("Failed to initialize MetaTrader 5")
    quit()

# Specify the symbol you want to check
symbols = mt5.symbols_get()
# Retrieve symbol information
for symbol in symbols:
    symbol_info = mt5.symbol_info(symbol.name)

    if symbol_info is not None:
        # Get the supported filling modes
        filling_modes = symbol_info.filling_mode

        # Map filling modes to human-readable names
        filling_mode_names = {
            0: "ORDER_FILLING_FOK",
            1: "ORDER_FILLING_IOC",
            2: "ORDER_FILLING_RETURN" ,
            3: "ORDER_FILLING_BOC"
        }
    
        print(f"Supported filling modes for {symbol.name} : {filling_modes}")
    else:
        print(f"Symbol {symbol} not found or not available.")

# Shut down MetaTrader 5 connection
mt5.shutdown()
