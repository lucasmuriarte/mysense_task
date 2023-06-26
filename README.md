## TAKE HOME TASK ##

You are NOT expected to complete all the tasks (100%) - as you are not expected to have prior experience using apache flink - that said the test is not designed to take too long. Try to spend three hours (it is not timed so you can spend more time if you like), if you feel your stuck and it's taking too long - that's fine, just try to get as far as you can (a useful way is to print intermediate results to console), and note your approach and thinking so we can discuss together during the third stage.

If anything is not clear in instructions, there are errors, environment is not building/starting etc. please do not hesitate to reach out 

The following documentation should help you with the tasks below that require the use of apache flink; in particular pyflink (Python API): https://nightlies.apache.org/flink/flink-docs-stable/

## Tasks: 

As said not a lot of coding is required to complete these tasks; however you would have to understand the necessary transformations for task A and B and then also 
understand the flink API to carry these out. As flink exposes many layers of APIs (from lower level to higher level) there could be a plethora of approaches to solve
these tasks (hence don't feel restricted) but to help you get started quickly, to ensure that docker environment is running properly on your computer and to also act as a guide:
a working example is provided in the form of a starter template and further tips have also bee been provided below. 


By copying to a new file (in ./src) and then modifying the starter template './src/start_template.py' write two pyflink applications that will take the data stored in ./data/input/events.json and perform the following tasks: 


    A. Extract nested heartrate data for cid '5f2cc245-9c8d-4c40-b764-9210d0e2ffb1' and output separately on a different line in json format in ./data/output - example output is provided in the file: ./data/example_output_task_A.txt

    The output should be of the format (json): {<cid:string>, <type:string>, <heartrate:int>, <hr_time:timestamp> }

    Tip: Example output DDL that can be used for this task A is provided in './src/example_input_taskA.py' - once the correct transformation for this task has occurred , you can use this to write data to a file in the directory requested above
    Tip: Writing intermediate output to console (using console as sink instead of file) is a useful way to debug and also track progress
    Tip: Try to complete task A first before moving on to task B, as the output for task A can be used as an intermediate step for B

    B. For cid '5f2cc245-9c8d-4c40-b764-9210d0e2ffb1' calculate the average heartrate in a ten minute time frame to be output in ./data/output - example output is provided in the file: ./data/example_output_task_B.txt

    The output should be of the format (json): {<cid:string>,<time_frame_start:timestamp>,<average_heart_rate:int>}

    Tip: If you choose to use the table api aggregations you will need to register 'event time' - amongst many possible approaches the two options are: 

        - Create a new pyflink app (using './src/start_template.py' as a base) with output from Task A as your 'input' and as 'output' have the required transformation, the example input DDL that can be used for this task is in './src/example_input_taskB.py'
        - Create a new pyflink app using the pyflink app for task A as the base and add more operations - (NB it is also possible to convert between tables and datastreams in pyflink - please refer to documentation )

    Tip: If using table API and if column names are lost during transformations , the 'alias' command is a useful shortcut to rename columns as desired (it is not necessary to use this)
    
    The following link provides more detail about working with 'event time': 
    
    https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/concepts/time_attributes/


## Instructions to run: 

 a.  Build the docker file using the following commands ```docker build . -t pyflink:mysense``` , if using Mac with silicon chip try using the command: ```docker build . -t pyflink:mysense --platform linux/amd64 ```

 b. Update or copy the code as necessary in ./src/main.py to perform tasks above - NB some tips have been provided at the top

 c. Run the following command to generate required output file: ```docker run  -v <path to directory>/data:/opt/heart_rate_flink/data  pyflink:mysense  /etc/poetry/bin/poetry run python /opt/heart_rate_flink/src/starter_template.py```


NB you could augment the command (if desired) in 3 to make an interactive shell and also add another volume mapping to map the source code directory with that in the docker container for quicker development (this should allow you to code in an your IDE of choice and execute your pyflink code in the docker container shell without having to rebuild; in addition as highlighted in 'Tip' above there should be options in pyflink to print to console to help with debugging) 


## Submission: 

Either create a new public github repo and share address or zip and then email your solution with the modified code  (there should be one python file for task A and one for task B) and also provide commands such as above to run and get desired output in the output directory. 
 
