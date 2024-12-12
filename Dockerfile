FROM python:3.11-slim

RUN apt-get update && \
  apt-get install -y fontconfig && \
  rm -rf /var/lib/apt/lists/*

# Allows docker to cache installed dependencies between builds
WORKDIR /tmp

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create the directory for custom fonts
RUN mkdir -p /usr/share/fonts/custom

# Copy the Gotham-Book.ttf font into the custom directory
COPY python_roh/dash/assets/GothamSSm-Medium.ttf /usr/share/fonts/custom/
COPY python_roh/dash/assets/GothamSSm-Book.ttf /usr/share/fonts/custom/
RUN fc-cache -fv
# Print the list of available fonts for logging
RUN fc-list | grep "Gotham"

# Sets the working directory in the container
WORKDIR /app

# Copies the local code to the container
COPY . .

CMD ["python", "main.py"]
