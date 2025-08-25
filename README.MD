## WIP python based Camunda job worker that uses docling to convert pdf files to markdown
This is a work in progress, it aims to help make sense of large pdf files and allow their content to be used in agentic processes

### Preparation and Installation
In order to run this project on your machine you will need to follow the installation instructions from [docling](https://github.com/docling-project/docling) 

#### Dependencies
I tested this project on a macbook with python3.13, it did not fly with python3.12 due to missing mps features for docling.

These installed prerequisits should keep you covered.
`pip install docling grpcio grpcio-tools`

and prefetch the models: [advanced options](https://docling-project.github.io/docling/usage/advanced_options/)

```
$ docling-tools models download
Downloading layout model...
Downloading tableformer model...
Downloading picture classifier model...
Downloading code formula model...
Downloading easyocr models...
Models downloaded into $HOME/.cache/docling/models.
```

#### update .env file
locate the .env file and update your api credentials accordingly

#### Import Connector Template
find the included ./CamundaConnectorTempate/Docling Converter.json and upload and publish it to a Camunda Modeler Webproject

### Usage
Start up the worker by simply running it via python:
`python worker.py`

Now you can create a Process, attach a start form with a file picker and point it to a variable name 'document' and add a task with the Docling Converter connector. Deploy the process and start it via Camunda Tasklist, upload a pdf file and see how Docling converts it for you. 

### additional thoughts
Keep in mind that this project is a work in progress and mainly aims to be a proof of concept, I'll try to find more time for refinement of it.