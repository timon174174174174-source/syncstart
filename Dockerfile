FROM node:22-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY server.js ./
COPY public ./public
RUN mkdir -p /data && chown node:node /data
USER node
ENV PORT=8080 DATA_FILE=/data/rooms.json
EXPOSE 8080
CMD ["node", "server.js"]
