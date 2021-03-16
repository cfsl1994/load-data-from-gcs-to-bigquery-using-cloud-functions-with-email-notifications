#python 3.7 lambda base image
FROM public.ecr.aws/lambda/python:3.7

# copy requirements.txt to container
COPY requirements.txt ./

# installing dependencies
RUN pip3 install -r requirements.txt

# Copy function code to container
COPY BeautifulSoup-Computrabajo.py ./

# setting your CMD to your handler filename.function_name
CMD [ "BeautifulSoup-Computrabajo.handler"]
