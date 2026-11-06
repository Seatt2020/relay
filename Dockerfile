FROM node:20-alpine
WORKDIR /app
COPY server.js package.json ./
EXPOSE 80
CMD ["node", "server.js"]
