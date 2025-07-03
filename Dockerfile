# Use a Node.js version that matches your package.json 'engines' field
FROM node:20-bullseye-slim

# Install system dependencies, including curl and unzip, before they are needed
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Install bun using the now-available curl command
RUN curl -fsSL https://bun.sh/install | bash \
    && mv /root/.bun /usr/local/bun \
    && ln -s /usr/local/bun/bin/bun /usr/local/bin/bun

# Set the working directory for the rest of the commands
WORKDIR /app

# Copy package management files first to leverage Docker layer caching
COPY package.json bun.lockb* ./

# Install dependencies using bun. --frozen-lockfile is best practice for CI/deployments.
RUN bun install --frozen-lockfile

# Copy the rest of your application code into the container
COPY . .

# Run the build script defined in your package.json
RUN bun run build

# Expose the port your application will run on (Hono's default is 3000)
EXPOSE 3000

# Define the command to start your application
CMD ["bun", "run", "start"]
