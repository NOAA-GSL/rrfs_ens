


import argparse
from distributed import Client
import functools
import os
import shutil
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

    for fhr in cla.fcst_hour:

        # Get perturbations
        if cla.inputpath:
            ens_path = cla.inputpath.format(fhr=fhr)
            ens_perts = compute_perturbations(ens_path)

        elif cla.perturbation_file:
            ens_perts = load_perturbations(cla.perturbation_file)

        else:
            print('No source of ensemble perturbations was provided. ' +
                  'Please call script with -p or -i option')

        # Write ens perturbations to a single file
        if cla.write_perturbations:
            if cla.perturbation_file:
                pass

        # Write ensmeble of full-state files
        if cla.ens_outfn_tmpl and cla.base_state:

            # Open base state file
            base_fpath = cla.base_state.format(fhr=fhr)
            base_state = xr.open_mfdataset(base_fpath)

            # Call the appropriate variables function
            variables = globals().get(f"{cla.vars}_variables")()
            print(variables)

            mem_fpaths = []
            mem_states = []

            for mem in range(0, ens_perts.dims['ens']):

                print(f'Preparing member {mem+1}')
                # Create output directory. Done here since it could be
                # per member.
                outputdir = cla.outputdir.format(mem=mem+1)
                os.makedirs(outputdir, exist_ok=True)

                # Identify member output file
                mem_fname = cla.ens_outfn_tmpl.format(fhr=fhr, mem=mem+1)
                mem_fpath = os.path.join(outputdir, mem_fname)
                mem_fpaths.append(mem_fpath)

                # Write update state to ensemble member file
                #print(f'Writing file: {mem_fpath}')


                ## Copy base state file to outputdir
                tic = time.perf_counter()
                shutil.copyfile(base_fpath, mem_fpath)
                toc = time.perf_counter()
                print(f'Copy time: {toc - tic}')
                #base_state = xr.open_fdataset(mem_fpath)

                pert = ens_perts[variables].sel(ens=mem)
                mem_states.append(base_state.update(base_state + pert))

                #tic = time.perf_counter()
                #ens_state.to_netcdf(mem_fpath, format='NETCDF4_CLASSIC',
                #        mode='a')
                #toc = time.perf_counter()
                #print(f'Write time: {toc - tic}')


        xr.save_mfdataset(mem_states, mem_fpaths,
                format='NETCDF4_CLASSIC', mode='a')

def atmo_variables():

    ''' Return list of atmospheric variables to be perturbed. '''

    return ['ps', 't', 'zh', 'sphum', 'u_w', 'v_w', 'u_s', 'v_s']


def bndy_variables():

    ''' Return the list of boundary variables to perturb. '''

    pert_vars = atmo_variables()
    bndy = ['_bottom', '_top', '_right', '_left']
    return [var + suffix for var in pert_vars for suffix in bndy]

def compute_perturbations(inpath):

    ''' Read in an ensemble dataset described by inpath, and return the
    ensemble perturbations about the mean. '''

    ens = xr.open_mfdataset(inpath,
                            combine='nested',
                            concat_dim='ens',
                            parallel=True,
                            )
    ens_mean = ens.mean(dim='ens')
    return ens - ens_mean

def check_perturbation_file(cla):

    ''' Check to ensure that no existing perturbation file will be
    overwritten. '''

    if cla.write_perturbations:
        if cla.perturbation_file:
            if os.path.exist(cla.perturbation_file):
                msg = 'The perturbation file exists. Will not overwrite it!'
                raise argparse.ArgumentError(msg)

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
