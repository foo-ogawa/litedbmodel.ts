// Generates interpreter/test-only Rust fixtures from the canonical SCP/BC declarations in ops.ts.
// It emits no JSON and is never part of native codegen-build output.

import { mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { generateRustExecutable } from '../../dist/scp/index.cjs';
import { buildOps, buildRelationLimitOracle, type BenchOp } from './ops';
import { ORM_DIALECTS, type OrmDialect } from './contract';
import { ddl, dropStatements, pgSeqResetStatements, seedStatements } from './orm-domain';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '../..');
const OUT = join(ROOT, 'rust/litedbmodel_oracle/src/generated');
const FIXTURE = join(OUT, 'fixture.rs');
const REGISTERED = ['rust-typed-native'];

function rustString(value: string): string {
  return `"${value
    .replaceAll('\\', '\\\\')
    .replaceAll('"', '\\"')
    .replaceAll('\n', '\\n')
    .replaceAll('\r', '\\r')
    .replaceAll('\t', '\\t')}"`;
}

function nodeLiteral(value: unknown): string {
  if (value === null || value === undefined) return 'Node::Null';
  if (typeof value === 'boolean') return `Node::Bool(${value})`;
  if (typeof value === 'number') return Number.isInteger(value) ? `Node::Int(${value}i64)` : `Node::Float(${value})`;
  if (typeof value === 'string') return `Node::Str(${rustString(value)}.to_string())`;
  if (Array.isArray(value)) return `Node::Array(vec![${value.map(nodeLiteral).join(',')}])`;
  const fields = Object.entries(value as Record<string, unknown>)
    .filter(([, field]) => field !== undefined)
    .map(([key, field]) => `(${rustString(key)}.to_string(), ${nodeLiteral(field)})`);
  return `Node::Object(vec![${fields.join(',')}])`;
}

function selected(dialect: OrmDialect): BenchOp[] {
  const byId = new Map(buildOps(dialect).map((op) => [op.id, op]));
  return ['nestedRelations', 'create', 'nestedCreate'].map((id) => {
    const op = byId.get(id);
    if (op === undefined) throw new Error(`canonical oracle op missing: ${id}/${dialect}`);
    return op;
  });
}

function format(path: string): void {
  const result = spawnSync('rustfmt', ['--edition', '2021', path], { encoding: 'utf8' });
  if (result.status !== 0) throw new Error(`rustfmt failed for ${path}: ${result.stderr}`);
}

function inlineLiteral(value: unknown): string {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'number') return String(value);
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  return `'${String(value).replaceAll("'", "''")}'`;
}

function inlineSeed(sql: string, params: readonly unknown[]): string {
  let index = 0;
  return sql.replaceAll('?', () => inlineLiteral(params[index++]));
}

function emitSetup(dialect: OrmDialect): void {
  const statements = [
    ...dropStatements(dialect),
    ...ddl(dialect),
    ...seedStatements(dialect).map(({ sql, params }) => inlineSeed(sql, params)),
    ...(dialect === 'postgres' ? pgSeqResetStatements() : []),
  ];
  const path = join(OUT, `setup_${dialect}.rs`);
  writeFileSync(path, `// GENERATED test-only ${dialect} setup from orm-domain.ts.\npub const STATEMENTS: &[&str] = &[\n    ${statements.map(rustString).join(',\n    ')}\n];\n`);
  format(path);
}

mkdirSync(OUT, { recursive: true });
const before = new Map(readdirSync(OUT).filter((name) => name.endsWith('.rs')).map((name) => [name, readFileSync(join(OUT, name), 'utf8')]));
const bundleArms: string[] = [];
const inputArms: string[] = [];
for (const dialect of ORM_DIALECTS) {
  for (const op of selected(dialect)) {
    bundleArms.push(`(${rustString(op.id)}, ${rustString(dialect)}) => ${nodeLiteral(op.bundle)},`);
    inputArms.push(`(${rustString(op.id)}, ${rustString(dialect)}) => ${nodeLiteral(op.input)},`);
    const nativePath = join(OUT, `${op.id}_${dialect}.rs`);
    writeFileSync(nativePath, generateRustExecutable(op.bundle, `${op.id}_${dialect}`, op.resolve, REGISTERED));
    format(nativePath);
  }
  emitSetup(dialect);
  const limit = buildRelationLimitOracle(dialect);
  bundleArms.push(`(${rustString(limit.id)}, ${rustString(dialect)}) => ${nodeLiteral(limit.bundle)},`);
  inputArms.push(`(${rustString(limit.id)}, ${rustString(dialect)}) => ${nodeLiteral(limit.input)},`);
  const modulePath = join(OUT, `relation_limit_${dialect}.rs`);
  writeFileSync(modulePath, generateRustExecutable(limit.bundle, `relation_limit_${dialect}`, limit.resolve, REGISTERED));
  format(modulePath);
}

const source = `// GENERATED from ops.ts by oracle-fixture-build.ts — interpreter/test-only.\nuse litedbmodel_interpreter::Node;\n\npub fn bundle(op: &str, dialect: &str) -> Node { match (op, dialect) {\n${bundleArms.join('\n')}\n_ => panic!("unknown oracle fixture {op}/{dialect}"),\n} }\n\npub fn input(op: &str, dialect: &str) -> Node { match (op, dialect) {\n${inputArms.join('\n')}\n_ => panic!("unknown oracle input {op}/{dialect}"),\n} }\n`;
writeFileSync(FIXTURE, source);
format(FIXTURE);
const after = new Map(readdirSync(OUT).filter((name) => name.endsWith('.rs')).map((name) => [name, readFileSync(join(OUT, name), 'utf8')]));
const names = new Set([...before.keys(), ...after.keys()]);
if (process.argv.includes('check') && [...names].some((name) => before.get(name) !== after.get(name))) {
  console.error('oracle-fixture-build: DRIFT');
  process.exit(1);
}
console.log('oracle-fixture-build: canonical interpreter/test fixtures are current');
