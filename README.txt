This project contains spark code using scala for louvain algorithm and  centrality measures like Degree centrality,closeness centrality,Vertex betweeness,edge betweeness and clustering coefficient.

Each of the above programs are under individual directories which is under the "programs" directory.

Input data for each program is in the respective directory named input.txt/in.txt

Outputs for the programs will be saved in the respective folder only.

For execution of above programs from the command line, get into the program's directory and then run the following command:

"sbt package"

this will create a jar file which can be found in the following path:

.....\target\scala-2.11\jar_filename.jar

after the jar file is created successfully,for a standalone system: run the following command:


"spark-submit --class Class_name_without_quotes --master local[2] jar_file_path command_line_args"

for a cluster system: run the following command:

"spark-submit --class Class_name_without_quotes --deploy-mode cluster --num-executors 5 /jar_file_path command_line_args"