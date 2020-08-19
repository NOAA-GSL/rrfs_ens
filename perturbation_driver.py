


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

def fhr_list(args):

    '''
    Given an arg list, return the sequence of forecast hours to process.
    The length of the list will determine what forecast hours are returned:
      Length = 1:   A single fhr is to be processed
      Length = 2:   A sequence of start, stop with increment 1
      Length = 3:   A sequence of start, stop, increment
      Length > 3:   List as is
    argparse should provide a list of at least one item (nargs='+').
    Must ensure that the list contains integers.
    '''

    args = args if isinstance(args, list) else [args]
    arg_len = len(args)
    if arg_len in (2, 3):
        return list(range(*args))

    return args

def parse_args():

    parser = argparse.ArgumentParser(
        description='''Ensemble perturbation manager.
        Read in an ensemble of NetCDF files or a single perturbation
        file to gather ensemble perturbations, then write the ensemble
        perturbations and/or the perturbations to disk.''')

    parser.add_argument(
        '-i',
        dest='inputpath',
        help='Input template path, including filenames. Used, if provided.',
        )
    parser.add_argument(
        '-f',
        dest='fcst_hour',
        default=[0],
        help='A list describing forecast hours. If one argument, a ' +
        'single forecast hour will be processed. If 2 or 3 arguments,' +
        ' a sequence of forecast hours [start, stop, [increment]] ' +
        'will be processed. If more than 3 arguments, the provided ' + 
        'is processed as-is.',
        nargs='+',
        type=int,
        )
    parser.add_argument(
        '-o',
        dest='outputdir',
        help='Output directory path',
        required=True,
        )
    parser.add_argument(
        '-e',
        dest='ens_outfn_tmpl',
        help='Template for the output ens filenames',
        )
    parser.add_argument(
        '-p',
        dest='perturbation_file',
        help='Full path to NetCDF perturbation file. Used as ' +
        'input when no -i option is provided, and output when' +
        ' the --write_perturbations flag is used.',
        )

    # Long options
    parser.add_argument(
            '--write_perturbations',
            help='If present, perturbations file described by ' +
            '-p argument will be written.'
            )
    return parser.parse_args()

if __name__ == '__main__':

    CLARGS = parse_args()
    CLARGS.fcst_hour = fhr_list(CLARGS.fcst_hour)

    main(args)
