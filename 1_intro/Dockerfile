# docker base image that we will use to build our image
FROM python:3.9.1

# install the required python packages
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2-binary pyarrow

# Set the working directory in the container
WORKDIR /app

# Copy the pipeline file to the working directory in the container
COPY ingest_data.py ingest_data.py

# define what to do when the container starts
ENTRYPOINT [ "python", "ingest_data.py" ]

