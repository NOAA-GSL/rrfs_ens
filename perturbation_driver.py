


import xarray as xr

ens_path = '/scratch1/BMC/wrfruc/chunhua/fv3sar-testing/code/FV3SAR-DA-05202020/expt_dirs/test_gsd_develop_gdas_0??/2019062006/INPUT/gfs_bndy.tile7.000.nc'

perturbed_variables = ['ps', 't', 'zh', 'sphum', 'u_w', 'v_w', 'u_s', 'v_s']
boundaries = ['_bottom', '_top', '_right', '_left']

bndy_variables  = []
for var in perturbed_variables:
    bndy_variables.extend([var+suffix for suffix in boundaries])

#ens = xr.open_mfdataset(ens_path, concat_dim='time', data_vars=bndy_variables, parallel=True, combine='nested')
ens = xr.open_mfdataset(ens_path, concat_dim='ensemble', parallel=True, combine='nested')

ens_mean = ens.mean(dim='ensemble')

ens_perturbations = ens - ens_mean

print(ens_perturbations)



