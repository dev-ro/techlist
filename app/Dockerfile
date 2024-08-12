# Use the official Python image from the Docker Hub
FROM python:3.12.4-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

# Copy only the requirements file first to leverage Docker cache
COPY requirements.txt /app/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir --disable-pip-version-check -r requirements.txt && \
    rm -rf /root/.cache/pip

# Copy the current directory contents into the container at /app
COPY . /app

# Use a shell script to start Streamlit with the correct port
RUN echo '#!/bin/bash\n\
PORT="${PORT:-8080}"\n\
streamlit run --server.port=$PORT --server.address=0.0.0.0 app.py' > /app/start.sh && \
chmod +x /app/start.sh

# Run the start script
CMD ["/app/start.sh"]