#!/bin/bash

jobname="ambience"
partition="supporters"

rm -rf ALL_SLURMS
mkdir ALL_SLURMS ## store all the SLURMs created to this location

files=(500_C_100 500_C_250 500_C_500 500_C_750 500_C_1K 500_C_1500 500_C_2K 500_C_2684) # (1250_C_100 1250_C_500 1250_C_500 1250_C_750 1250_C_1K 1250_C_1500 1250_C_2K 1250_C_2684) # 750_C_250 750_C_1K 750_C_2684 1000_C_100 1000_C_250 1000_C_500 1000_C_750 1000_C_1K 1000_C_2684)
nodes=(20) # 30 40 60)

work=$1
kway=3
red=$2
split=$3
tasks=1




func()
{
directive="#!/bin/bash\n\n#SBATCH --partition=$1\n#SBATCH --nodes=$2\n#SBATCH --ntasks-per-node=$3\n#SBATCH --job-name=$jobname\n##SBATCH --time-min=06:20:00\n#SBATCH --mail-user=rajaramr@buffalo.edu\n#SBATCH --output=result/%j.out\n#SBATCH --error=result/error_%j.out\n#SBATCH --exclusive"
echo -e $directive
}


params="echo \"EXPERIMENT INPUT ----------------------\"\necho \"#filename \" \$1\necho \"#operation \" \$2\necho \"#kway \" \$3\necho \"#reducers \" \$4\necho \"#splits \" \$5\necho \"nodes \" \$SLURM_NNODES\necho \" cores \" \$SLURM_NTASKS_PER_NODE\necho \"Jpb specs\" \$6\n\n"


setup="module load java/1.6.0_22\nmodule load hadoop/2.5.1\nmodule load hbase/0.98.10.1\nmodule load myhadoop/0.30b\n\n"

setup1="export MH_SCRATCH_DIR=\$SLURMTMPDIR\nexport HADOOP_CONF_DIR=\$SLURM_SUBMIT_DIR/config-\$SLURM_JOBID\nexport HBASE_CONF_DIR=\$SLURM_SUBMIT_DIR/config-\$SLURM_JOBID\nmkdir hbase-logs-\$SLURM_JOBID\nmkdir hist-\$SLURM_JOBID\nexport MY_LOG_DIR=\$SLURM_SUBMIT_DIR/hbase-logs-\$SLURM_JOBID\n"

setup2="NPROCS=\`srun --nodes=\${SLURM_NNODES} bash -c 'hostname' |wc -l\`\n\$MH_HOME/bin/myhadoop-configure.sh >/dev/null\ncp \$HBASE_HOME/conf/hbase-env.sh-sample \$HBASE_CONF_DIR/hbase-env.sh\ncp \$HBASE_HOME/conf/hbase-site.xml-sample \$HBASE_CONF_DIR/hbase-site.xml\ncp \$HADOOP_CONF_DIR/slaves \$HBASE_CONF_DIR/regionservers\n\nNODE_A=\`cat  \$HADOOP_CONF_DIR/slaves | awk 'NR==1{print;exit}'\`\nNODE_B=\`cat  \$HADOOP_CONF_DIR/slaves | awk 'NR==2{print;exit}'\`\nNODE_C=\`cat  \$HADOOP_CONF_DIR/slaves | awk 'NR==3{print;exit}'\`\necho \$NODE_B > \$HBASE_CONF_DIR/backup-masters >/dev/null\nsed -i 's:NODE-A:'"\$NODE_A"':g' \$HBASE_CONF_DIR/hbase-site.xml\nsed -i 's:NODE-B:'"\$NODE_B"':g' \$HBASE_CONF_DIR/hbase-site.xml\nsed -i 's:NODE-C:'"\$NODE_C"':g' \$HBASE_CONF_DIR/hbase-site.xml\nsed -i 's:MY_HBASE_SCRATCH:'"\$SLURMTMPDIR"':g' \$HBASE_CONF_DIR/hbase-site.xml\nsed -i 's:MY_LOG_DIR:'"\$MY_LOG_DIR"':g' \$HBASE_CONF_DIR/hbase-env.sh\nls -l \$HADOOP_CONF_DIR >/dev/null\nsleep 15\n\n\$HADOOP_HOME/sbin/start-all.sh >/dev/null\nsleep 15\n\n\$HBASE_HOME/bin/start-hbase.sh --config=\$HBASE_CONF_DIR >/dev/null\nsleep 15\n\necho \"execution -f \$1 -i -99 -o \$2 -k \$3 -r \$4 -s \$5\"\n\$HADOOP_HOME/bin/hadoop --config \$HADOOP_CONF_DIR jar AMBIENCE.jar -f \$1 -i -99 -o \$2 -k \$3 -r \$4 -s \$5 -j \$SLURM_JOBID\n"

cpyJhist="\necho \"Copying job history data back\"\n$HADOOP_HOME/bin/hdfs --config \$HADOOP_CONF_DIR dfs -copyToLocal /tmp/hadoop-yarn/staging/history/done_intermediate/rajaramr/*.* hist-\$SLURM_JOBID\n"

stop="\$HBASE_HOME/bin/stop-hbase.sh >/dev/null\n\$HADOOP_HOME/sbin/stop-all.sh >/dev/null\n\$MH_HOME/bin/myhadoop-cleanup.sh >/dev/null"


for node in "${nodes[@]}"
do
   :
	func "general-compute" $node $tasks > SLURM_$node
	echo -e $params >> SLURM_$node
	echo -e $setup >> SLURM_$node
	echo -e $setup1 >> SLURM_$node
	echo -e $setup2 >> SLURM_$node
	echo -e $cpyJhist >> SLURM_$node
	echo -e $stop >> SLURM_$node
	for file in "${files[@]}"
	do :
		echo SLURM_$node $file $work $kway $red $split
		sbatch SLURM_$node $file $work $kway $red $split
	done
done

mv SLURM* ALL_SLURMS/
