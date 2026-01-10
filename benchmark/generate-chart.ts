#!/usr/bin/env npx ts-node
/**
 * Generate SVG benchmark chart from CSV results
 * 
 * Usage:
 *   npx ts-node generate-chart.ts
 *   # or
 *   npm run chart
 */

import * as fs from 'fs/promises';
import * as path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

interface BenchmarkRow {
  operation: string;
  orm: string;
  median: number;
  iqr: number;
  stdDev: number;
  min: number;
  max: number;
  iterations: number;
}

// ORM order and colors
const ORM_ORDER = ['litedbmodel', 'Kysely', 'Drizzle', 'TypeORM', 'Prisma'];
const ORM_COLORS: Record<string, string> = {
  'litedbmodel': '#3b82f6',  // Blue
  'Kysely': '#22c55e',       // Green
  'Drizzle': '#f59e0b',      // Amber
  'TypeORM': '#ef4444',      // Red
  'Prisma': '#8b5cf6',       // Purple
};

// Maximum relative value to show (bars exceeding this are truncated)
const MAX_DISPLAY_RATIO = 5.0;

async function parseCSV(csvPath: string): Promise<BenchmarkRow[]> {
  const content = await fs.readFile(csvPath, 'utf-8');
  const lines = content.trim().split('\n');
  const rows: BenchmarkRow[] = [];
  
  // Skip header
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    // Parse CSV with quoted fields
    const match = line.match(/"([^"]+)","([^"]+)",([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+)/);
    if (match) {
      rows.push({
        operation: match[1],
        orm: match[2],
        median: parseFloat(match[3]),
        iqr: parseFloat(match[4]),
        stdDev: parseFloat(match[5]),
        min: parseFloat(match[6]),
        max: parseFloat(match[7]),
        iterations: parseInt(match[8]),
      });
    }
  }
  
  return rows;
}

// Escape XML special characters
function escapeXml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function generateSVG(rows: BenchmarkRow[]): string {
  // Group by operation
  const operations = [...new Set(rows.map(r => r.operation))];
  
  // Calculate litedbmodel baseline for relative comparison
  const baselineByOperation = new Map<string, number>();
  for (const op of operations) {
    const liteRow = rows.find(r => r.operation === op && r.orm === 'litedbmodel');
    if (liteRow) {
      baselineByOperation.set(op, liteRow.median);
    }
  }
  
  // Chart dimensions
  const margin = { top: 60, right: 150, bottom: 80, left: 220 };
  const barHeight = 12;
  const barGap = 4;
  const groupGap = 25;  // Space between operation groups (reduced)
  const ormCount = ORM_ORDER.length;
  const groupHeight = (barHeight + barGap) * ormCount;
  const chartHeight = operations.length * (groupHeight + groupGap) - groupGap;
  const chartWidth = 400;  // Fixed chart width
  const width = margin.left + chartWidth + margin.right;
  const height = margin.top + chartHeight + margin.bottom;
  
  // Scale function (capped at MAX_DISPLAY_RATIO)
  const scale = (value: number) => (Math.min(value, MAX_DISPLAY_RATIO) / MAX_DISPLAY_RATIO) * chartWidth;
  
  let svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${width} ${height}" font-family="system-ui, -apple-system, sans-serif">
  <!-- White background -->
  <rect width="100%" height="100%" fill="white"/>
  <style>
    .title { font-size: 18px; font-weight: bold; fill: #1f2937; }
    .subtitle { font-size: 12px; fill: #6b7280; }
    .op-label { font-size: 11px; fill: #374151; }
    .orm-label { font-size: 9px; fill: #6b7280; }
    .value-label { font-size: 8px; fill: #374151; }
    .value-overflow { font-size: 8px; fill: #dc2626; font-weight: bold; }
    .axis-label { font-size: 10px; fill: #9ca3af; }
    .grid-line { stroke: #e5e7eb; stroke-width: 1; }
    .baseline { stroke: #22c55e; stroke-width: 2; stroke-dasharray: 6,3; }
    .overflow-line { stroke: #dc2626; stroke-width: 1; }
    .legend-text { font-size: 10px; fill: #374151; }
    .na-text { font-size: 8px; fill: #9ca3af; font-style: italic; }
  </style>
  
  <!-- Title -->
  <text x="${width / 2}" y="25" text-anchor="middle" class="title">ORM Benchmark Comparison</text>
  <text x="${width / 2}" y="45" text-anchor="middle" class="subtitle">Relative speed (litedbmodel = 1.0, lower is faster)</text>
  
  <!-- Chart area -->
  <g transform="translate(${margin.left}, ${margin.top})">
    <!-- Grid lines -->
`;
  
  // Vertical grid lines
  const gridSteps = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0];
  for (const step of gridSteps) {
    if (step <= MAX_DISPLAY_RATIO) {
      const x = scale(step);
      svg += `    <line x1="${x}" y1="0" x2="${x}" y2="${chartHeight}" class="grid-line" />\n`;
      svg += `    <text x="${x}" y="${chartHeight + 15}" text-anchor="middle" class="axis-label">${step.toFixed(1)}</text>\n`;
    }
  }
  
  // Overflow indicator at right edge
  svg += `    <text x="${chartWidth + 5}" y="${chartHeight + 15}" text-anchor="start" class="axis-label" fill="#dc2626">5+</text>\n`;
  
  // Baseline line (1.0)
  const baselineX = scale(1.0);
  svg += `    <line x1="${baselineX}" y1="-5" x2="${baselineX}" y2="${chartHeight + 5}" class="baseline" />\n`;
  
  // Bars for each operation
  for (let opIdx = 0; opIdx < operations.length; opIdx++) {
    const op = operations[opIdx];
    const groupY = opIdx * (groupHeight + groupGap);
    const baseline = baselineByOperation.get(op) || 1;
    
    // Operation label
    svg += `    <text x="-10" y="${groupY + groupHeight / 2}" text-anchor="end" dominant-baseline="middle" class="op-label">${escapeXml(op)}</text>\n`;
    
    // Bars for each ORM
    for (let ormIdx = 0; ormIdx < ORM_ORDER.length; ormIdx++) {
      const orm = ORM_ORDER[ormIdx];
      const y = groupY + ormIdx * (barHeight + barGap);
      const color = ORM_COLORS[orm];
      
      // Get data from CSV
      const row = rows.find(r => r.operation === op && r.orm === orm);
      const median = row?.median ?? null;
      
      if (median === null) {
        svg += `    <text x="5" y="${y + barHeight / 2 + 1}" dominant-baseline="middle" class="na-text">N/A</text>\n`;
        continue;
      }
      
      const relative = median / baseline;
      const isOverflow = relative > MAX_DISPLAY_RATIO;
      const barWidth = scale(relative);
      
      // Bar
      svg += `    <rect x="0" y="${y}" width="${barWidth}" height="${barHeight}" fill="${color}" rx="2" />\n`;
      
      // Overflow indicator (diagonal stripes at end of bar)
      if (isOverflow) {
        svg += `    <line x1="${barWidth - 3}" y1="${y}" x2="${barWidth}" y2="${y + barHeight}" class="overflow-line" />\n`;
        svg += `    <line x1="${barWidth - 6}" y1="${y}" x2="${barWidth - 3}" y2="${y + barHeight}" class="overflow-line" />\n`;
      }
      
      // Value label
      const labelX = barWidth + 5;
      const labelClass = isOverflow ? 'value-overflow' : 'value-label';
      const overflowMarker = isOverflow ? '⚠️ ' : '';
      svg += `    <text x="${labelX}" y="${y + barHeight / 2 + 1}" dominant-baseline="middle" class="${labelClass}">${overflowMarker}${relative.toFixed(2)}x (${median.toFixed(2)}ms)</text>\n`;
    }
  }
  
  svg += `  </g>
  
  <!-- Legend -->
  <g transform="translate(${margin.left + chartWidth + 20}, ${margin.top})">
`;
  
  for (let i = 0; i < ORM_ORDER.length; i++) {
    const orm = ORM_ORDER[i];
    const y = i * 20;
    svg += `    <rect x="0" y="${y}" width="14" height="14" fill="${ORM_COLORS[orm]}" rx="2" />\n`;
    svg += `    <text x="20" y="${y + 11}" class="legend-text">${orm}</text>\n`;
  }
  
  // Overflow legend
  svg += `    <line x1="0" y1="${ORM_ORDER.length * 20 + 10}" x2="14" y2="${ORM_ORDER.length * 20 + 10}" class="overflow-line" />\n`;
  svg += `    <text x="20" y="${ORM_ORDER.length * 20 + 14}" class="legend-text" fill="#dc2626">&gt;5x (truncated)</text>\n`;
  
  svg += `  </g>
</svg>`;
  
  return svg;
}


async function main() {
  const csvPath = path.join(__dirname, 'results', 'benchmark-results.csv');
  const svgPath = path.join(__dirname, '..', 'docs', 'benchmark-chart.svg');
  
  console.log(`📊 Reading benchmark results from: ${csvPath}`);
  
  try {
    const rows = await parseCSV(csvPath);
    console.log(`   Found ${rows.length} data points for ${new Set(rows.map(r => r.operation)).size} operations`);
    
    const svg = generateSVG(rows);
    
    await fs.mkdir(path.dirname(svgPath), { recursive: true });
    await fs.writeFile(svgPath, svg);
    
    console.log(`✅ SVG chart saved to: ${svgPath}`);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      console.error('❌ No benchmark results found. Run the benchmark first:');
      console.error('   npm run benchmark');
    } else {
      throw error;
    }
  }
}

main().catch(console.error);

