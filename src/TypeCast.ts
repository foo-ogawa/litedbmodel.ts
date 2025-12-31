/**
 * litedbmodel - Type Cast Interface
 *
 * Provides database-agnostic type conversion utilities.
 * The actual implementation is selected based on the configured driver.
 */

// ============================================
// Type Cast Interface
// ============================================

/**
 * Interface for type casting functions.
 * Different database drivers may have different implementations.
 */
export interface TypeCastFunctions {
  castToDatetime(val: unknown): Date | null;
  castToBoolean(val: unknown): boolean | null;
  castToIntegerArray(val: unknown): number[];
  castToNumericArray(val: unknown): (number | null)[];
  castToStringArray(val: unknown): string[];
  castToBooleanArray(val: unknown): (boolean | null)[];
  castToDatetimeArray(val: unknown): (Date | null)[];
  castToJson(val: unknown): Record<string, unknown> | unknown[] | null;
}

// ============================================
// Default Implementation (Database Agnostic)
// ============================================

/**
 * Default type cast implementation that works for most databases.
 * This is used when no specific driver is configured.
 */
const defaultTypeCast: TypeCastFunctions = {
  castToDatetime(val: unknown): Date | null {
    if (val === null || val === undefined) return null;
    if (val instanceof Date) return val;
    if (typeof val === 'string' || typeof val === 'number') {
      const date = new Date(val);
      return isNaN(date.getTime()) ? null : date;
    }
    return null;
  },

  castToBoolean(val: unknown): boolean | null {
    if (val === null || val === undefined) return null;
    if (typeof val === 'boolean') return val;
    if (typeof val === 'number') return val !== 0;
    if (typeof val === 'string') {
      const lower = val.toLowerCase();
      if (lower === 'true' || lower === 't' || lower === '1') return true;
      if (lower === 'false' || lower === 'f' || lower === '0') return false;
    }
    return null;
  },

  castToIntegerArray(val: unknown): number[] {
    if (val === null || val === undefined) return [];
    if (Array.isArray(val)) {
      return val.map((v) => {
        const n = parseInt(String(v), 10);
        return isNaN(n) ? 0 : n;
      });
    }
    if (typeof val === 'string') {
      try {
        const parsed = JSON.parse(val);
        if (Array.isArray(parsed)) {
          return parsed.map((v) => {
            const n = parseInt(String(v), 10);
            return isNaN(n) ? 0 : n;
          });
        }
      } catch {
        // Try PostgreSQL array format
        return parsePostgresArray(val).map((v) => {
          const n = parseInt(v, 10);
          return isNaN(n) ? 0 : n;
        });
      }
    }
    return [];
  },

  castToNumericArray(val: unknown): (number | null)[] {
    if (val === null || val === undefined) return [];
    if (Array.isArray(val)) {
      return val.map((v) => {
        if (v === null || v === undefined) return null;
        const n = parseFloat(String(v));
        return isNaN(n) ? null : n;
      });
    }
    if (typeof val === 'string') {
      try {
        const parsed = JSON.parse(val);
        if (Array.isArray(parsed)) {
          return parsed.map((v) => {
            if (v === null) return null;
            const n = parseFloat(String(v));
            return isNaN(n) ? null : n;
          });
        }
      } catch {
        return parsePostgresArray(val).map((v) => {
          if (v === 'NULL' || v === '') return null;
          const n = parseFloat(v);
          return isNaN(n) ? null : n;
        });
      }
    }
    return [];
  },

  castToStringArray(val: unknown): string[] {
    if (val === null || val === undefined) return [];
    if (Array.isArray(val)) return val.map((v) => String(v));
    if (typeof val === 'string') {
      try {
        const parsed = JSON.parse(val);
        if (Array.isArray(parsed)) return parsed.map((v) => String(v));
      } catch {
        return parsePostgresArray(val);
      }
    }
    return [];
  },

  castToBooleanArray(val: unknown): (boolean | null)[] {
    if (val === null || val === undefined) return [];
    if (Array.isArray(val)) {
      return val.map((v) => defaultTypeCast.castToBoolean(v));
    }
    if (typeof val === 'string') {
      try {
        const parsed = JSON.parse(val);
        if (Array.isArray(parsed)) {
          return parsed.map((v) => defaultTypeCast.castToBoolean(v));
        }
      } catch {
        return parsePostgresArray(val).map((v) => defaultTypeCast.castToBoolean(v));
      }
    }
    return [];
  },

  castToDatetimeArray(val: unknown): (Date | null)[] {
    if (val === null || val === undefined) return [];
    if (Array.isArray(val)) {
      return val.map((v) => defaultTypeCast.castToDatetime(v));
    }
    if (typeof val === 'string') {
      try {
        const parsed = JSON.parse(val);
        if (Array.isArray(parsed)) {
          return parsed.map((v) => defaultTypeCast.castToDatetime(v));
        }
      } catch {
        return parsePostgresArray(val).map((v) => defaultTypeCast.castToDatetime(v));
      }
    }
    return [];
  },

  castToJson(val: unknown): Record<string, unknown> | unknown[] | null {
    if (val === null || val === undefined) return null;
    if (typeof val === 'object') return val as Record<string, unknown> | unknown[];
    if (typeof val === 'string') {
      try {
        return JSON.parse(val);
      } catch {
        return null;
      }
    }
    return null;
  },
};

/**
 * Parse PostgreSQL array literal string
 */
function parsePostgresArray(literal: string): string[] {
  if (!literal || literal === '{}') return [];

  let content = literal.trim();
  if (content.startsWith('{') && content.endsWith('}')) {
    content = content.slice(1, -1);
  }

  if (content === '') return [];

  const result: string[] = [];
  let current = '';
  let inQuotes = false;
  let escaped = false;

  for (let i = 0; i < content.length; i++) {
    const char = content[i];

    if (escaped) {
      current += char;
      escaped = false;
      continue;
    }

    if (char === '\\') {
      escaped = true;
      continue;
    }

    if (char === '"') {
      inQuotes = !inQuotes;
      continue;
    }

    if (char === ',' && !inQuotes) {
      result.push(current);
      current = '';
      continue;
    }

    current += char;
  }

  if (current !== '' || result.length > 0) {
    result.push(current);
  }

  return result;
}

// ============================================
// Global Type Cast Provider
// ============================================

let currentTypeCast: TypeCastFunctions = defaultTypeCast;

/**
 * Set the type cast implementation to use.
 * Call this when initializing the database connection.
 * @internal
 */
export function setTypeCastImpl(impl: TypeCastFunctions): void {
  currentTypeCast = impl;
}

/**
 * Reset to default type cast implementation.
 * @internal
 */
export function resetTypeCastImpl(): void {
  currentTypeCast = defaultTypeCast;
}

/**
 * Get the current type cast implementation.
 */
export function getTypeCast(): TypeCastFunctions {
  return currentTypeCast;
}

// ============================================
// Exported Functions (Use Current Implementation)
// ============================================

export function castToDatetime(val: unknown): Date | null {
  return currentTypeCast.castToDatetime(val);
}

export function castToBoolean(val: unknown): boolean | null {
  return currentTypeCast.castToBoolean(val);
}

export function castToIntegerArray(val: unknown): number[] {
  return currentTypeCast.castToIntegerArray(val);
}

export function castToNumericArray(val: unknown): (number | null)[] {
  return currentTypeCast.castToNumericArray(val);
}

export function castToStringArray(val: unknown): string[] {
  return currentTypeCast.castToStringArray(val);
}

export function castToBooleanArray(val: unknown): (boolean | null)[] {
  return currentTypeCast.castToBooleanArray(val);
}

export function castToDatetimeArray(val: unknown): (Date | null)[] {
  return currentTypeCast.castToDatetimeArray(val);
}

export function castToJson(val: unknown): Record<string, unknown> | unknown[] | null {
  return currentTypeCast.castToJson(val);
}

