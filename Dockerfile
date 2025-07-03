# Use Node 18 as the base image for compatibility with DuckDB
FROM node:18-bullseye-slim

# Install Bun
RUN curl -fsSL https://bun.sh/install | bash \
    && mv /root/.bun /usr/local/bun \
    && ln -s /usr/local/bun/bin/bun /usr/local/bin/bun

# Install necessary system libraries
RUN apt-get update && apt-get install -y \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy package.json and bun.lockb (if you have one)
COPY package.json bun.lockb* ./

# Install dependencies
RUN bun install

# Copy the rest of your application
COPY . .

# Run database setup scripts for key and stis
RUN bun run build

# Set environment variable
ENV NODE_ENV=production

# Start the application
CMD ["bun", "run", "src/index.ts"]

# Expose the port
EXPOSE 3000
