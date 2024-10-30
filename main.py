import subprocess

scripts = ["0.create-table.py", "1.1enrichissement-adress2.py", "1.2enriched_clients.py", "2.1enriched.py", "2.2enrichedCSVandTable.py", "3.1enriched.py", "3.2enriched-graphes.py"]

for script in scripts:
    subprocess.run(["python", script])