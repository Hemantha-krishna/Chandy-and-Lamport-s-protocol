# CS6378 Advanced Operating System – Project 1 Complete

This project implements a **distributed system with the MAP protocol** in Java.
Each node reads configuration parameters, establishes TCP connections with its neighbors, and exchanges messages following the MAP protocol rules.

## Files
- `Node.java` – The main source code implementing all protocols.
- `config.txt` – The configuration file defining system parameters and topology.
- `launcher.sh`- Shell script to compile the source code and run all nodes on remote hosts.
- `cleanup.sh` - Shell script to stop all running Node processes and remove generated files.
- `config-<node_id>.out` - Output files containing the vector timestamps recorded during snapshots.
- `README.md` – this documentation.	

## Compile: Compilation is handled automatically by the launcher, but can be done manually:
javac Node.java

## Start the System: Execute the launcher script to compile and run all nodes in the background.
chmod +x launcher.sh
./launcher.sh

## Cleanup and Stop: Execute the cleanup script to terminate processes and remove files.
chmod +x cleanup.sh
./cleanup.sh

## Output Format
The output file for process j is named <config_name>-j.out.

