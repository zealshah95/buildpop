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
    no_tasks = 2
    mem_per_cpu = 16000
    if i <= 10:
        partition = "m40-long"
        job_name = "neg_patch_1"
    elif 10<i<=20:
        partition = "titanx-long"
        job_name = "neg_patch_2"
    else:
        partition = "matlab"
        job_name = "neg_patch_3"

    f.write('''#!/bin/bash
#
#SBATCH --job-name={}
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=zshah@umass.edu
#SBATCH --ntasks={}
#SBATCH -p {}
#SBATCH --mem-per-cpu={}
srun python neg_patch_extraction.py {} {}
    '''.format(job_name,no_tasks,partition,mem_per_cpu,i,batch_files))

# NOTE: w+ will create a file and write it if it doesnt exist
# or else will open an existing file and write it

#################Create Runscript################################
run_script = open("runscript.sh", "w+")
run_script.write('''#!/bin/bash\n''')
for i in range(batch_files):
    run_script.write('''sbatch slurm-{}.sh\n'''.format(i))