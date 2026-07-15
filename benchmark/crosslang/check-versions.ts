// Cross-language bench PREFLIGHT (epic #44): assert every language's behavior-contracts
// dependency is pinned to the SAME version. The cross-lang comparison is only valid if all
// five language runtimes execute against the identical bc release — a drift (e.g. go.sum stuck
// on an old tag while npm/crates moved) silently invalidates the numbers. Fail-closed: throw
// with a per-manifest diff so the mismatch is LOUD, never a silently-skewed run.

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

interface ManifestPin {
  readonly label: string;
  readonly file: string;
  readonly re: RegExp;
}

// Each language's bc version SoT (the version string is capture group 1).
const PINS: readonly ManifestPin[] = [
  { label: 'npm (ts)', file: 'package.json', re: /"behavior-contracts":\s*"[\^~]?([0-9][0-9A-Za-z.\-+]*)"/ },
  // The rust runtime is NATIVE-ONLY: it pins bc via the inline-table form with default-features =
  // false (drops bc's serde_json-pulling `ir` feature), so the version lives inside the table.
  { label: 'crates (rust runtime)', file: 'rust/Cargo.toml', re: /^behavior-contracts\s*=\s*\{\s*version\s*=\s*"([0-9][0-9A-Za-z.\-+]*)"/m },
  { label: 'crates (rust bench adapter)', file: 'benchmark/crosslang/adapters/rust/Cargo.toml', re: /^behavior-contracts\s*=\s*"([0-9][0-9A-Za-z.\-+]*)"/m },
  // The dedicated JSON-free codegen adapter crate (default-features = false; version is in the inline table).
  { label: 'crates (rust codegen adapter)', file: 'benchmark/crosslang/adapters/rust-codegen/Cargo.toml', re: /^behavior-contracts\s*=\s*\{\s*version\s*=\s*"([0-9][0-9A-Za-z.\-+]*)"/m },
  { label: 'go module', file: 'go/go.mod', re: /behavior-contracts\/go\s+v([0-9][0-9A-Za-z.\-+]*)/ },
  { label: 'pypi (python)', file: 'python/pyproject.toml', re: /behavior-contracts==([0-9][0-9A-Za-z.\-+]*)/ },
];

/**
 * Read every language's pinned bc version and assert they are identical. Throws (fail-closed)
 * if any manifest is unreadable, any pin is missing, or the versions disagree.
 * @param repoRoot absolute path to the litedbmodel repo root.
 */
export function assertBcVersionsAligned(repoRoot: string): string {
  const found: { label: string; version: string }[] = [];
  const problems: string[] = [];

  for (const pin of PINS) {
    const path = resolve(repoRoot, pin.file);
    let text: string;
    try {
      text = readFileSync(path, 'utf8');
    } catch {
      problems.push(`${pin.label}: cannot read ${pin.file}`);
      continue;
    }
    const m = pin.re.exec(text);
    if (m === null) {
      problems.push(`${pin.label}: no behavior-contracts version pin found in ${pin.file}`);
      continue;
    }
    found.push({ label: pin.label, version: m[1] });
  }

  const versions = new Set(found.map((f) => f.version));
  if (problems.length > 0 || versions.size !== 1) {
    const table = found.map((f) => `    ${f.label.padEnd(28)} ${f.version}`).join('\n');
    const probs = problems.length > 0 ? `\n  MISSING/UNREADABLE:\n    ${problems.join('\n    ')}` : '';
    throw new Error(
      `cross-lang bench PREFLIGHT: behavior-contracts version MISMATCH across languages — the ` +
        `cross-language comparison is invalid unless all runtimes use the SAME bc release.\n` +
        `  Pinned versions:\n${table}${probs}\n` +
        `  Align every manifest to one version (and refresh lockfiles: go.sum / Cargo.lock / package-lock.json).`,
    );
  }
  return found[0].version;
}
