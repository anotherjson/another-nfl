# Evidence Dashboard Dockerfile (Simplified)
FROM node:18-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Evidence project files
COPY visualizations/evidence/ ./

# Create a simple static server setup since Evidence CLI is not available
COPY visualizations/evidence/package.json ./package.json

# Create a basic server script
RUN echo 'const http = require("http");\n\
const fs = require("fs");\n\
const path = require("path");\n\
\n\
const server = http.createServer((req, res) => {\n\
  if (req.url === "/" || req.url === "/index.html") {\n\
    res.writeHead(200, { "Content-Type": "text/html" });\n\
    res.end(`\n\
    <html>\n\
      <head><title>NFL Analytics - Evidence Dashboard</title></head>\n\
      <body>\n\
        <h1>NFL Analytics Dashboard</h1>\n\
        <p>Evidence dashboard placeholder - Dashboard-as-code framework</p>\n\
        <p>This would normally show SQL-generated reports and visualizations</p>\n\
        <ul>\n\
          <li><a href="/team-performance">Team Performance</a></li>\n\
          <li><a href="/fantasy">Fantasy Analytics</a></li>\n\
          <li><a href="/players">Player Analysis</a></li>\n\
        </ul>\n\
      </body>\n\
    </html>\n\
    `);\n\
  } else {\n\
    res.writeHead(200, { "Content-Type": "text/html" });\n\
    res.end(`<h1>NFL Evidence Dashboard</h1><p>Page: ${req.url}</p><a href="/">Back to Home</a>`);\n\
  }\n\
});\n\
\n\
const PORT = 3000;\n\
server.listen(PORT, "0.0.0.0", () => {\n\
  console.log(`Evidence dashboard placeholder running on port ${PORT}`);\n\
});' > server.js

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash evidence
RUN chown -R evidence:evidence /app
USER evidence

# Expose Evidence port
EXPOSE 3000

# Run simple server
CMD ["node", "server.js"]