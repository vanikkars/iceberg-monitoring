FROM node:20-slim

WORKDIR /app

# Install dependencies (pages/ and sources/ are mounted at runtime)
COPY package.json ./
RUN npm install --legacy-peer-deps --include=dev
# Patch @evidence-dev/trino to handle Trino's precision types (varchar(N),
# timestamp(N) with time zone) and convert timestamp strings to Date objects.
RUN node - <<'EOF'
const fs = require('fs');
const p = 'node_modules/@evidence-dev/trino/index.cjs';
let c = fs.readFileSync(p, 'utf8');
// varchar(N) → STRING
c = c.replace("case 'varchar':", "case typeLower.match(/^varchar/)?.input:");
// timestamp / timestamp(N) / timestamp(N) with time zone → DATE
c = c.replace("case 'timestamp':\n\t\tcase 'timestamp with time zone':", "case typeLower.match(/^timestamp/)?.input:");
// Convert "YYYY-MM-DD HH:mm:ss.SSS UTC" strings to Date objects for DATE columns
c = c.replace(
  "let vAdjusted = typeof v === 'object' && v !== null ? v.toString() : v;\n\t\t\trowObj[columnTypes[i].name] = vAdjusted;",
  "let vAdjusted = typeof v === 'object' && v !== null ? v.toString() : v;\n\t\t\tif (columnTypes[i].evidenceType === 'date' && typeof vAdjusted === 'string') {\n\t\t\t\tconst d = new Date(vAdjusted.replace(' UTC', 'Z').replace(' ', 'T'));\n\t\t\t\tif (!isNaN(d.getTime())) vAdjusted = d;\n\t\t\t}\n\t\t\trowObj[columnTypes[i].name] = vAdjusted;"
);
fs.writeFileSync(p, c);
console.log('Patched @evidence-dev/trino connector');
EOF

# Copy config (pages/ and sources/ are volume-mounted for live editing)
COPY evidence.config.yaml ./

EXPOSE 3000

# Run sources (pulls data from DuckDB/Nessie) then start dev server
CMD ["sh", "-c", "npm run sources && npm run dev"]