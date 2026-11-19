# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/azure-functions/python:4-python3.12
ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true
WORKDIR /home/site/wwwroot
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Supply the verified official Linux x64 AzCopy v10 binary before building.
COPY --chmod=0755 bin/azcopy /usr/local/bin/azcopy
COPY host.json function_app.py copy_job.py copy_reliability.py copy_recovery.py ./

ENV COPY_RECOVERY_SCHEDULE="0 */15 * * * *"
