## Uncomment below for WSGI server with command Docker single image only
# FROM ubuntu
# ENV PYTHONUNBUFFERED=1
# RUN apt update -y && \
#     apt install -y python3-pip python3-dev
# RUN mkdir /app
# WORKDIR /app
# COPY . .
# RUN pip install --no-cache-dir -r requirements.txt
# # VOLUME [ "/app" ]
# # WORKDIR /app
# ENTRYPOINT ["gunicorn"]
# CMD [ "-w","1","-b", "0.0.0.0:80","wsgi:app" ]


## Uncomment below for WSGI server using docker-compose
FROM ubuntu:20.04
ENV TZ=Asia/Kolkata
ARG DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
RUN apt update -y && \
    apt-get -y install tzdata && \
    apt install -y python3-pip python3-dev
RUN mkdir /app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install setuptools==59.1.1
RUN pip install --no-cache-dir python-aes256==1.0.5
WORKDIR /app


##### Fast API Docker File
# FROM ubuntu
# ENV PYTHONUNBUFFERED=1

# RUN apt update -y && \
#     apt install -y python3-pip python3-dev

# RUN mkdir /app
# WORKDIR /app
# COPY requirements.txt t_requirements.txt
# RUN pip install --no-cache-dir -r t_requirements.txt
# VOLUME [ "/app" ]
# WORKDIR /app

# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]


#gunicorn -w 1 -b 0.0.0.0:8000 wsgi:server --reload
