import grpc
import requests
import json
from gateway_pb2 import ActivateJobsResponse, ActivateJobsRequest, ActivatedJob, CompleteJobRequest, CompleteJobResponse
from gateway_pb2_grpc import GatewayStub
from dotenv import load_dotenv
load_dotenv()
import os

# Docling settings
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption

artifacts_path = "~/.cache/docling/models"
docs_path = "./docs/"

pipeline_options = PdfPipelineOptions(artifacts_path=artifacts_path)
doc_converter = DocumentConverter(format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)})

# Access settings Camunda
client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")
cluster_id = os.getenv("cluster_id")
region = os.getenv("region")

def get_access_token(url, client_id, client_secret):
    response = requests.post(
        url,
        data={"grant_type": "client_credentials", 
              "audience": "zeebe.camunda.io",
              "client_id": client_id,
              "client_secret":client_secret},
        auth=(client_id, client_secret),
    )
    return response.json()["access_token"]

channel = grpc.secure_channel(f"{cluster_id}.{region}.zeebe.camunda.io:443", grpc.ssl_channel_credentials())
access_token = get_access_token("https://login.cloud.camunda.io/oauth/token", client_id, client_secret)
headers = [('authorization', f'Bearer {access_token}')]
client = GatewayStub(channel)

def activate_job(jobType):
    print(f"activating jobs of type {jobType}...")
    activate_jobs_request = ActivateJobsRequest(
        type=jobType,
        maxJobsToActivate=1,
        timeout=60000,
        requestTimeout=60000
    )
    activate_jobs_response: ActivateJobsResponse = client.ActivateJobs(activate_jobs_request, metadata=headers)
    jobsResponse = list(activate_jobs_response)
    activatedJob: ActivatedJob = jobsResponse[0].jobs[0]
    print(f"activated job: {activatedJob.key}")

    return activatedJob

def complete_job(activatedJob, variables):
    complete_job_request: CompleteJobRequest = CompleteJobRequest(
        jobKey= activatedJob.key,
        variables= json.dumps(variables)
    )
    complete_job_response: CompleteJobResponse = client.CompleteJob(complete_job_request, metadata=headers)

    return complete_job_response

def download_doc(document):
    documentId = document["documentId"]
    contentHash = document["contentHash"]
    fileMetaData = document["metadata"]
    fileName = fileMetaData["fileName"]
    params = {"Authorization":f"Bearer {access_token}"}
    url = f"https://{region}.zeebe.camunda.io:443/{cluster_id}/v2/documents/{documentId}?contentHash={contentHash}"
    response = requests.get(url, headers=params)
    with open(f"{docs_path}{fileName}", "wb") as f:
        f.write(response.content)

    return fileName

if __name__ == "__main__":
    try:
        print("starting worker...")
        while True:
            try:
                job: ActivatedJob = activate_job("converter.docling")
                variables = json.loads(job.variables)
                outputName = variables["outputVarName"]
                print(f"output varname: {outputName}")
                document = variables["document"][0]
                doc_name = download_doc(document)
                # convert with docling
                result = doc_converter.convert(f'{docs_path}{doc_name}')
                markdown = result.document.export_to_markdown()
                variables[outputName] = markdown
                complete_job(job, variables)
            except Exception as e:
                print(f"job worker error: {e}")

    except Exception as e:
        print(f"Error: {e}")