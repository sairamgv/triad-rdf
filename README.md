# TriAD: A Distributed Shared Nothing RDF Engine
TriAD (for Triple Asynchronous and Distributed) is a distributed RDF engine built on top of asynchronous communication protocol and multi-threaded query execution framework.

## Install
Please check the INSTALL file for detailed instructions

## Running 
```
mpirun [cluster_info] ./tools/triad [opt]
cluster_info:
-np <num_hosts>		Pseudo cluster mode 
-hosts <hosts_list>	Cluster mode with comma seperated hosts list
opt:
-s			Slient (skips maintaining dictionary and does not print results)
-i			Input (N3/NT file or directory location)
-p			Partition information (for summary mode)
-d			Dictionary (for summary mode)
-parts			Number of partitions (for summary mode)
```
```
Eg: 
1. mpirun -hosts host1,host2,host3 ./tools/triad -i example.n3  -s (basic mode)
2. mpirun -hosts host1,host2,host3 ./tools/triad -i example.n3 -p example.g.mf.part.2 -d example.g_dict -parts 2 -s (summary mode)
```
## Generating partition and dictionary files for summary mode
1. Command for generating metis format file and dictionary file
  `./tools/transformat   <Location of n3/nt file/dir>   <prefix for the metis/dictionary file>`
2. Use metis on generated metis format file for generating partition file

Eg:
1. `./tools/transformat  example.n3  example.g`
   -- generates example.g.mf and example.g_dict
2. `./tools/gpmetis example.g.mf 2`
   -- generates example.g.mf.part.2

   
   





