'''
Python utility that leverages xarray to load an ensemble of NetCDF
files, extract the perturbations, and apply them to a base state.

Supports reading/writing a perturbation file instead of a full ensmeble
of files.

'''

import argparse
import functools
import os
import time

import xarray as xr

def timer(func):

    ''' Decorator function that provides an elapsed time for a method. '''

    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        tic = time.perf_counter()
        value = func(*args, **kwargs)
        toc = time.perf_counter()
        elapsed_time = toc - tic
        print(f"Elapsed time: {elapsed_time:0.4f} seconds")
        return value
    return wrapper_timer

@timer
def main(cla):

    ''' Main function that takes a Namespace structure of command line
    arguments (cla) and generates the requested ensemble output. '''

    for fhr in cla.fcst_hour:

        # Call the appropriate variables function
        variables = globals().get(f"{cla.vars}_variables")()

        # Get perturbations
        if cla.inputpath:
            ens_path = cla.inputpath.format(fhr=fhr)
            ens_perts = compute_perturbations(ens_path, variables)

        elif cla.perturbation_file:
            ens_perts = load_perturbations(cla.perturbation_file)

        else:
            print('No source of ensemble perturbations was provided. ' +
                  'Please call script with -p or -i option')

        # Write ens perturbations to a single file
        if cla.write_perturbations:
            if cla.perturbation_file:
                ens_perts.to_netcdf(cla.perturbation_file)

        # Write ensmeble of full-state files
        if cla.ens_outfn_tmpl and cla.base_state:

            # Open base state file
            base_fpath = cla.base_state.format(fhr=fhr)
            base_state = xr.open_mfdataset(base_fpath)

            for mem in range(0, ens_perts.dims['ens']):

                print(f'Preparing member {mem+1}')
                # Create output directory. Done here to allow support
                # for mem field in format string.
                outputdir = cla.outputdir.format(mem=mem+1)
                os.makedirs(outputdir, exist_ok=True)

                # Identify member output file, and generate a list
                mem_fname = cla.ens_outfn_tmpl.format(fhr=fhr, mem=mem+1)
                mem_fpath = os.path.join(outputdir, mem_fname)

                # Write full ensemble members to disk. One at a time allows
                # compression, and should give similar performance as
                # save_mfdataset() without a dask cluster.
                full_mem = ens_perts[variables].sel(ens=mem) + base_state
                comp = {'zlib': True, 'complevel': 9}
                encoding = {var: comp for var in full_mem.data_vars}
                full_mem.to_netcdf(mem_fpath, encoding=encoding)

def atmo_variables():

    ''' Return list of atmospheric variables to be perturbed. '''

    return ['ps', 't', 'zh', 'sphum', 'u_w', 'v_w', 'u_s', 'v_s']


def bndy_variables():

    ''' Return the list of boundary variables to perturb. '''

    pert_vars = atmo_variables()
    bndy = ['_bottom', '_top', '_right', '_left']
    return [var + suffix for var in pert_vars for suffix in bndy]

def compute_perturbations(inpath, data_vars):

    ''' Read in an ensemble dataset described by inpath, and return the
    ensemble perturbations about the mean. '''

    ens = xr.open_mfdataset(inpath,
                            combine='nested',
                            compat='override',
                            concat_dim='ens',
                            coords='minimal',
                            data_vars=data_vars,
                            parallel=True,
                            )
    ens_mean = ens.mean(dim='ens')
    return ens - ens_mean

def check_perturbation_file(cla):

    ''' Check to ensure that no existing perturbation file will be
    overwritten. '''

    if cla.write_perturbations:
        if cla.perturbation_file:
            if os.path.exists(cla.perturbation_file):
                msg = 'The perturbation file exists. Will not overwrite it!'
                raise argparse.ArgumentTypeError(msg)

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

def load_perturbations(inpath):

    ''' Open a single NetCDF file of perturbations. '''

    return xr.open_dataset(inpath)

def parse_args():

    ''' Parse arguments to script using argparse. '''

    parser = argparse.ArgumentParser(
        description='''Ensemble perturbation manager.
        Read in an ensemble of NetCDF files or a single perturbation
        file to gather ensemble perturbations, then write the ensemble
        perturbations and/or the perturbations to disk.''')

    # Short options
    parser.add_argument(
        '-b',
        dest='base_state',
        help='Full path the base state on which to add perturbations.',
        )
    parser.add_argument(
        '-e',
        dest='ens_outfn_tmpl',
        help='Output ensemble filenames. Template fields: mem. ' +
        'Full ensmble is written to disk if this argument is present.',
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
        '-i',
        dest='inputpath',
        help='Input path, including filename. Template fields: fhr. ' +
        'Used, if provided.',
        )
    parser.add_argument(
        '-o',
        dest='outputdir',
        help='Output directory path',
        required=True,
        )
    parser.add_argument(
        '-p',
        dest='perturbation_file',
        help='Full path to NetCDF perturbation file. Used as ' +
        'input when no -i option is provided, and output when' +
        ' the --write_perturbations flag is used.',
        )
    parser.add_argument(
        '-v',
        choices=['atmo', 'bndy', 'sfc'],
        dest='vars',
        help='Type of variables to process',
        required=True,
        )

    # Long options
    parser.add_argument(
        '--write_perturbations',
        action='store_true',
        help='If present, perturbations file described by ' +
        '-p argument will be written.'
        )
    return parser.parse_args()

def sfc_variables():

    ''' Return the list of surface variables to perturb. '''

    # HRRRE perturbed variables include soil moisture, veg frac, albedo,
    # and emissivity. Emissivity not included here.
    return ['smc', 'vfrac', 'alvsf', 'alvwf', 'alnsf', 'alnwf']

if __name__ == '__main__':

    CLARGS = parse_args()
    CLARGS.fcst_hour = fhr_list(CLARGS.fcst_hour)

    check_perturbation_file(CLARGS)

    main(CLARGS)
