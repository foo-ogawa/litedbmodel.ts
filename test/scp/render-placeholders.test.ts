import { describe, it, expect } from 'vitest';
import { renderPlaceholders } from '../../src/scp/makesql/handler';

/**
 * #42 — the PG `?`→`$N` pass must be QUOTE-AWARE: a `?` inside a single-quoted SQL
 * string literal is not a placeholder. Byte-identical to the old naive form for SQL with
 * no literal `?` (all compiled forms today), and correct where a literal `?` appears
 * (e.g. a Raw-SQL escape hatch) — where the naive form would mis-number.
 */
describe('renderPlaceholders (#42 quote-aware ?→$N)', () => {
  it('numbers real placeholders left-to-right (no literal ?) — byte-identical to naive', () => {
    expect(renderPlaceholders('WHERE a = ? AND b = ?', 'postgres')).toBe('WHERE a = $1 AND b = $2');
  });

  it('does NOT renumber (12 placeholders → $1..$12)', () => {
    const q = Array(12).fill('?').join(', ');
    expect(renderPlaceholders(q, 'postgres')).toBe(Array.from({ length: 12 }, (_, i) => `$${i + 1}`).join(', '));
  });

  it('DECISIVE — a `?` inside a string literal is NOT converted', () => {
    // the naive `replace(/\?/g)` would wrongly emit `note = '$1' AND x = $2`
    expect(renderPlaceholders("WHERE note = 'what?' AND x = ?", 'postgres')).toBe(
      "WHERE note = 'what?' AND x = $1",
    );
  });

  it('mixed literal and real placeholders keep correct numbering', () => {
    expect(renderPlaceholders("SELECT '?', ?, 'a?b', ?", 'postgres')).toBe("SELECT '?', $1, 'a?b', $2");
  });

  it('MySQL / SQLite leave `?` unchanged (incl. literal)', () => {
    expect(renderPlaceholders("WHERE note = 'what?' AND x = ?", 'mysql')).toBe("WHERE note = 'what?' AND x = ?");
    expect(renderPlaceholders("WHERE note = 'what?' AND x = ?", 'sqlite')).toBe("WHERE note = 'what?' AND x = ?");
  });
});
