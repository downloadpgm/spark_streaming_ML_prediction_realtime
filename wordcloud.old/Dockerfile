FROM mkenjis/ubpyspk_img

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=US/Central

RUN apt-get update && apt-get install -y python3-pip

RUN pip3 install tweepy nltk wordcloud matplotlib
