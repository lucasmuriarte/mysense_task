## TAKE HOME TASK ##

The results of the task can be seen in the data/output folder. These files are the result of executing the exercise.py file. To be able to run this file you need to have a python environment with pyflink or run it inside the docker.

## Tasks: 
The modified code is inside exercise.py file. This file executes the __task a__ and __task b__ one after the other. The output from __task a__ is used for __task b__. Two functions separate the code to make clear which part of each file performs the actions. There are other files from where a class and some functions are imported that this file uses to be able to perform the task.

## Instructions to run Docker: 

 a.  Build the docker file using the following commands ```docker build . -t pyflink:mysense``` , if using Mac with silicon chip try using the command: ```docker build . -t pyflink:mysense --platform linux/amd64 ```

 b. Include or copy the code in ./src/exercise.py; ./src/udf_helpers.py and ./src/input_output_tables.py 

 c. Run the following command to generate the required output file: ```docker run  -v <path to directory>/data:/opt/heart_rate_flink/data  pyflink:mysense  /etc/poetry/bin/poetry run python /opt/heart_rate_flink/src/exercise.py```


## Instructions to run Locally if there are problems running inside Docker:

In case there are problems running the exercise.py script inside Docker. create a python environment with apache-flink = "1.17.0" and run the exercise script to direct the program to read from a local directory ```<path to directory>``` ( the one that you would include in the docker volume) run ```pyhton3 exercise.py --data-path= <path to directory>```

## Remarks: 

I manage to build and add the different volumes for code and data but I had trouble running the starter template and modified version on Docker due to some permission error (If the same problems are faced run the file locally in a dedicated python enviroment). I do not fully understand this error but I believe this is the most relevant part of it: ```Caused by: java.nio.file.AccessDeniedException: /opt/heart_rate_flink/data/output/starter_result/.part-e8d533d5-4113-4cbf-bd78-54352d979b2f-1-0.inprogress.aaa96617-841c-41f5-8e43-9e390c4faa21```. To run inside docker I ended entering inside the container and run the exercise.py file. I spend time understanding the origin of this error but did not manage to find a clear explanation.

Understanding the task and requirements was not complicated. I found it challenging to understand the errors of Pyflink when there was something wrong. I really enjoy doing this challenge and understanding Pyflink.

