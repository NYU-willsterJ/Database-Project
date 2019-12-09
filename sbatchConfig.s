#!/bin/bash
#
##SBATCH --nodes=1
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --time=24:00:00
#SBATCH --mem=8GB
#SBATCH --job-name=test
#SBATCH --output=slurm_%j.out

module load python3/intel/3.7.3

pip3 install zodb --user
python3 ./main.py
