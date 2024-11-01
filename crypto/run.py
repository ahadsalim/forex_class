import multiprocessing

def run_script(script_name):
    import subprocess
    subprocess.run(["python", script_name])

if __name__ == "__main__":
    processes = []
    for script in ["crypto/update_symbols.py", "crypto/make_portfo.py", "crypto/update_portfo.py"]:
        p = multiprocessing.Process(target=run_script, args=(script,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()