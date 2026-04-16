FROM node:20-alpine

WORKDIR /app

# Install dependencies first (layer cached unless package.json changes)
COPY package.json package-lock.json ./
RUN npm ci

# Copy source
COPY . .

EXPOSE 3000

# Start Next.js dev server bound to all interfaces
CMD ["npm", "run", "dev", "--", "--hostname", "0.0.0.0"]
