FROM ubuntu:20.0.4
COPY geckodriver ./
RUN CHMOD +3 ./geckodriver
RUN sudo apt upgrade
RUN sudo apt-get update
RUN sudo apt-get install firefox=86.0+build3-0ubuntu0.20.04.1
RUN install python3.8
RUN install python3-pip
RUN pip3 install
COPY script ./
ENV time_frame, start_date, end_date
CMD python3 ./script