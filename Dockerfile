# syntax=docker/dockerfile:1
# Creating container to run jupyter notebooks from conda environment
FROM continuumio/miniconda3

# Refresh apt directories
RUN apt-get update -y; && \
    apt-get upgrade -y;

# Copy and install dependencies
COPY environment.yml environment.yml
RUN conda env create -f environment.yml

# Pull the environment name out of the environment.yml
RUN echo "source activate $(head -1 environment.yml | cut -d' ' -f2)" > ~/.bashrc
ENV PATH /opt/conda/envs/$(head -1 environment.yml | cut -d' ' -f2)/bin:$PATH

# Expose a port for jupyter notebooks
EXPOSE 8888

# Specify entry point for jupyter notebooks
ENTRYPOINT ["jupyter", "notebook","--ip=0.0.0.0","--allow-root", "--no-browser"]

# Copy source files from the package directory
WORKDIR /home/john/projects/DeepCVR/deepcvr
COPY . .