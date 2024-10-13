## README

Aside from the website visualization, plots using matplotlib are also provided in tools under this directory. 

### Config
Before ploting, all csv files must be generated using `generate_csv.py` in the upper directory. Then, take a look at `_config.py` and change the settings to fit your needs. 

`ROOT_PATH` is the base diretory where all taskvine logs are stored, the default is `logs` in the upper directory. 

`LOGS` is a list of logs that will be ploted by all the scripts, all scripts will take it in and plot figures by it. 

`SAVE_TO` is the target directory that your plotted figures will be saved to.

`LOG_TITLES` defines the titles of each subgraph, by default it is the name of the target log. Make sure it has the same number of that of `LOGS`


### Examples

`accumulated_disk_usage.py` plots the overall accumulated usage of the storage, which is the sum of all workers' disk usage over time

![ASC](imgs/ASC.png)

`individual_workers_disk_usage.py` plots the per-worker disk usage over time

![WSC](imgs/WSC.png)

`file_consumer_count.py` plots the number of consumer tasks of each output file. One output file is strictly produced by one task

![file_consumer_count](imgs/file_consumer_count.png)

`file_retention_time.py` plots the retention time of each file, starting from when it was generated to when it was totally consumed and staged out from the cluster 

![FRR](imgs/FRR.png)

`task_execution_time_cdf.py` plots the CDF of execution time of each task.

![task_execution_time_cdf](imgs/task_execution_time_cdf.png)

