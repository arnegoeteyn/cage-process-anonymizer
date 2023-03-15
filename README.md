# Template for a process to be deployed in the datacage

Example implementing a CSV analyzer in a confidential computing environment

This example shows how to use the Presidio Analyzer and Anonymizer
to detect and anonymize PII in a CSV file.
It uses the BatchAnalyzerEngine to analyze the CSV file, and the
BatchAnonymizerEngine to anonymize the requested columns.
Note that currently BatchAnonymizerEngine is not part of the anonymizer package,
and is defined in this and the batch_processing notebook.
https://github.com/microsoft/presidio/blob/main/docs/samples/python/batch_processing.ipynb

Content of csv file:
id,name,city,comments
1,John,New York,called him yesterday to confirm he requested to call back in 2 days
2,Jill,Los Angeles,accepted the offer license number AC432223
3,Jack,Chicago,need to call him at phone number 212-555-5555

"""

## Content of this repo
Apart from this file, this repo contains :

| | |
|----------|-------------|
| `requirements.txt` | the python dependencies to install, as this is a python project |
| `index.py` | the entry point executable to start the process, as declared in `datavillage.yaml`. In this example, it waits for one message on the local redis queue, processes it with `process.py`, then exits |
| `process.py` | the code that actually processes incoming events |
| `test.py` | some local tests |
| `Dockerfile` | a dockerfile to bundle the project and it's dependencies into a self contained docker image |
| `.github/workflows/release_docker_image.yaml` | sample code to build and publish the docker image via github actions |

## Deployment process
What happens when such a repo is pushed on a github repository ?

The dockerfile is being used to build a docker image that is then published on the github container registry.
You can then refer to this docker image via its registry address to make the data cage download and run it in a confidential environment

## Execution process

When starting the docker container, the following environment variables are made available :
 
| variable | description |
|----------|-------------|
| DV_TOKEN |        |
| DV_APP_ID |       |
| DV_CLIENT_ID |       |
| DV_URL |       |
| REDIS_SERVICE_HOST |       |
| REDIS_SERVICE_PORT |       |
| PII_CSV_PATH | OPTIONAL; file from data provider containing PII data |
| PII_CSV_ANONYMIZED_PATH | OPTIONAL; File without PII data created as output to data consumer |

The process execution is event-driven : triggering events are sent on a local Redis queue; the executable should 
subscribe to that queue and handle events appropriately.

The events are of the form 
```
{
    userIds: <optional list of user IDs>,
    jobId: <ID of the job that this event is bound to>,
    trigger: <type of event>,
}
```


