import sys
import multiprocessing
import os
import subprocess

def run_script(script_name):
    # Run the script using the current Python interpreter
    subprocess.run([sys.executable, script_name])

if __name__ == "__main__":
    # Verify we're in the virtual environment
    if not hasattr(sys, 'real_prefix') and not sys.base_prefix != sys.prefix:
        print("Please activate the virtual environment before running this script.")
        sys.exit(1)
    
    processes = []
    for script in ["forex/make_portfo.py", "forex/update_portfo.py"]:
        p = multiprocessing.Process(target=run_script, args=(script,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()