# #!/bin/bash
import os, sys
import pathlib
import subprocess
import argparse, textwrap
import datetime


def run_pipeline(cl, mf, cr=8):
    """ Runs the pipeline on a list of cases.
    The order of the pipeline is:
    1. extractb0.py
    2. antsRegistration.py
    3. maskprocessing.py
    4. postprocessing.py

    Parameters
    ----------
    cl: str
        path to caselist.txt
    mf : str
         path to model folder
    cr : int
         number of processes to use
    """
    t0 = datetime.datetime.now()
    # use subprocess to run the pipeline
    subprocess.run(["python", "extractb0.py", "-i", cl, "-nproc", str(abs(cr-2))])
    subprocess.run(["python", "antsRegistration.py", "-i", cl, "-f", mf, "-nproc", str(cr)])
    subprocess.run(["python", "maskprocessing.py", "-i", cl, "-f", mf])
    subprocess.run(["python", "postprocessing.py", "-i", cl])

    # print the total time taken to run the pipeline and memory usage
    t1 = datetime.datetime.now()
    print("Total time taken to run the pipeline: ", t1 - t0)
    print("Memory usage: ", os.popen('ps -o rss= -p %d' % os.getpid()).read())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
            This script runs the pipeline on a list of cases.
            The order of the pipeline is:
            1. extractb0.py
            2. antsRegistration.py
            3. maskprocessing.py
            4. postprocessing.py

            Example:
            python run_pipeline.py -i /path/to/caselist.txt -f /path/to/model_folder
            '''))
    parser.add_argument('-i', action='store', dest='caselist', type=str,
                        help="txt file containing list of /path/to/dwi, one path in each line")
    parser.add_argument('-f', action='store', dest='model_folder', type=str,
                        help="folder containing the trained models")
    parser.add_argument('-nproc', type=int, dest='cr', default=8, help='number of processes to use')

    try:
        args = parser.parse_args()
        if len(sys.argv) == 1:
            parser.print_help()
            parser.error('too few arguments')
            sys.exit(0)

    except SystemExit:
        sys.exit(0)

    if args.cr > os.cpu_count():
        print(f'Warning: npoc is greater than the number of cores available. Setting npoc to {os.cpu_count()}')
        args.cr = os.cpu_count()

    caselist = None
    model_folder = None

    if args.caselist:
        f = pathlib.Path(args.caselist)
        if f.exists():
            print("File exist")
            caselist = args.caselist
        else:
            print("File not found")
            sys.exit(1)
    if args.model_folder:
        f = pathlib.Path(args.model_folder)
        if f.exists():
            print("File exist")
            model_folder = args.model_folder
        else:
            print("File not found")
            sys.exit(1)

    run_pipeline(caselist, model_folder, args.cr)
