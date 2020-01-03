################################################################
################################################################
# creates n no. of batch files as specified in 'batch_files'
# content of each batch file is provided in f.write()
# It also produces a runscript which can be used to run
# all the batch scripts together
# job name will remain the same for all the files.
# this makes it easier to cancel the jobs by one name.
################################################################
################################################################
batch_files = 30

#################Create Slurm Files#############################
for i in range(batch_files):
    f= open("slurm-{}.sh".format(i),"w+")
    if i <= 10:
        f.write('''#!/bin/bash
#
#SBATCH --job-name=slicing_db_part1
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=zshah@umass.edu
#SBATCH --ntasks=1
#SBATCH -p m40-long
#SBATCH --mem-per-cpu=15000
srun python patch_extraction.py {} {}
    '''.format(i,batch_files))
    elif 10<i<=20:
        f.write('''#!/bin/bash
#
#SBATCH --job-name=slicing_db_part2
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=zshah@umass.edu
#SBATCH --ntasks=1
#SBATCH -p matlab
#SBATCH --mem-per-cpu=15000
srun python patch_extraction.py {} {}
    '''.format(i,batch_files))
    else:
        f.write('''#!/bin/bash
#
#SBATCH --job-name=slicing_db_part3
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=zshah@umass.edu
#SBATCH --ntasks=1
#SBATCH -p titanx-long
#SBATCH --mem-per-cpu=15000
srun python patch_extraction.py {} {}
    '''.format(i,batch_files))

# NOTE: w+ will create a file and write it if it doesnt exist
# or else will open an existing file and write it

#################Create Runscript################################
run_script = open("runscript.sh", "w+")
run_script.write('''#!/bin/bash\n''')
for i in range(batch_files):
    run_script.write('''sbatch slurm-{}.sh\n'''.format(i))