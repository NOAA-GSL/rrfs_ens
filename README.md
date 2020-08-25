# RRFS Ensemble Toolbox

This toolbox is meanto to house the tools necessary to interact with an FV3-LAM ensemble. 

## Ensemble perturbation extraction

Extract and write the set of ensemble perturbations from an ensemble of NetCDF files from chgres_cube.

## Ensemble recentering

Apply ensmble perturbations to a base state on the model grid from an ensemble or a perturbations file.



# Getting Started

On Hera, activate the appropriate conda environment:

```
module use -a /contrib/miniconda3/modulefiles
module load miniconda3
conda activate spp_vx
```

Use the -h option to see all available command line options:

```
python perturbation_generator.py -h
```

## Example for boundary files at 0, 3, 6 hours recentered ens member 1:

### Submitted as a slurm job:

```
#!/bin/bash


#SBATCH -t 01:30:00
#SBATCH --qos=batch
#SBATCH --account=user-account
#SBATCH --nodes=1-1
#SBATCH --job-name=ens_pert
#SBATCH --exclusive

export OMP_NUM_THREADS=24

ens_base=path/to/top/level/dir

python perturbation_driver.py \
  -i "$ens_base/test_gsd_develop_gdas_0??/2019062006/INPUT/gfs_bndy.tile7.{fhr:03d}.nc" \
  -b "$ens_base/test_gsd_develop_gdas_001/2019062006/INPUT/gfs_bndy.tile7.{fhr:03d}.nc" \
  -e 'out.{mem:02d}.{fhr:03d}h.bdy.nc' \
  -f 0 7 3 \
  -o ./test \
  -v sfc
```


