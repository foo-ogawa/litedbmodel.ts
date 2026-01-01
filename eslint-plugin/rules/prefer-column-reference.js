/**
 * @fileoverview Prefer Model.column reference over hardcoded column names in conditions
 * @description Warns when column names appear to be hardcoded in condition strings
 */

"use strict";

/** @type {import('eslint').Rule.RuleModule} */
module.exports = {
  meta: {
    type: "suggestion",
    docs: {
      description: "Prefer using Model.column reference over hardcoded column names in condition strings",
      category: "Best Practices",
      recommended: true,
    },
    schema: [
      {
        type: "object",
        properties: {
          // Minimum length for column name to trigger warning (avoid false positives like 'id')
          minColumnNameLength: {
            type: "number",
            default: 3,
          },
          // Additional column names to always check (e.g., 'id')
          alwaysCheckColumns: {
            type: "array",
            items: { type: "string" },
            default: [],
          },
        },
        additionalProperties: false,
      },
    ],
    messages: {
      hardcodedColumnName:
        "Column name '{{columnName}}' appears to be hardcoded. Consider using Model.{{columnName}} for refactoring safety.",
      hardcodedColumnNameWithSuggestion:
        "Column name '{{columnName}}' appears to be hardcoded. Consider using {{suggestion}} for refactoring safety.",
    },
  },

  create(context) {
    const options = context.options[0] || {};
    const minColumnNameLength = options.minColumnNameLength ?? 3;
    const alwaysCheckColumns = new Set(options.alwaysCheckColumns || []);

    // Methods where we check condition strings
    const conditionMethods = new Set([
      "find",
      "findOne",
      "findFirst",
      "findById",
      "update",
      "delete",
      "count",
      "inSubquery",
      "notInSubquery",
      "exists",
      "notExists",
    ]);

    // Methods that take raw SQL as first argument
    const rawSqlMethods = new Set([
      "query",
    ]);

    // Methods that take object with sql property
    const sqlObjectMethods = new Set([
      "withQuery",
    ]);

    // SQL operators that follow column names
    const sqlOperators = [
      "=",
      "!=",
      "<>",
      ">",
      "<",
      ">=",
      "<=",
      "LIKE",
      "ILIKE",
      "IN",
      "NOT",
      "IS",
      "BETWEEN",
      "AND",
      "OR",
    ];

    // Pattern to detect potential column names in SQL
    // Matches: column_name followed by space and operator, or column_name preceded by operator
    // snake_case or camelCase identifiers
    const columnPatternBefore = new RegExp(
      `\\b([a-z][a-z0-9_]*(?:_[a-z0-9]+)*)\\s*(?:${sqlOperators.join("|")})`,
      "gi"
    );
    const columnPatternAfter = new RegExp(
      `(?:${sqlOperators.join("|")})\\s*([a-z][a-z0-9_]*(?:_[a-z0-9]+)*)\\b`,
      "gi"
    );
    // Also match ORDER BY, GROUP BY patterns
    const orderByPattern = /\b(?:ORDER|GROUP)\s+BY\s+([a-z][a-z0-9_]*(?:_[a-z0-9]+)*)/gi;

    /**
     * Get model name from call expression (for condition methods)
     */
    function getCallerModel(node) {
      let current = node;
      while (current) {
        if (current.type === "CallExpression" &&
            current.callee?.type === "MemberExpression" &&
            current.callee.object?.type === "Identifier") {
          const methodName = current.callee.property?.name;
          if (conditionMethods.has(methodName)) {
            return current.callee.object.name;
          }
        }
        current = current.parent;
      }
      return null;
    }

    /**
     * Get model name from raw SQL method call (query, withQuery, etc.)
     */
    function getRawSqlCallerModel(node) {
      let current = node;
      while (current) {
        if (current.type === "CallExpression" &&
            current.callee?.type === "MemberExpression" &&
            current.callee.object?.type === "Identifier") {
          const methodName = current.callee.property?.name;
          if (rawSqlMethods.has(methodName) || sqlObjectMethods.has(methodName)) {
            return current.callee.object.name;
          }
        }
        current = current.parent;
      }
      return null;
    }

    /**
     * Check if this is a static QUERY property assignment
     */
    function isStaticQueryProperty(node) {
      // Check if this is: static QUERY = '...'
      if (node.parent?.type === "PropertyDefinition" &&
          node.parent.static === true &&
          node.parent.key?.type === "Identifier" &&
          node.parent.key.name === "QUERY") {
        return true;
      }
      return false;
    }

    /**
     * Get class name from static property
     */
    function getClassNameFromStaticProperty(node) {
      let current = node.parent;
      while (current) {
        if (current.type === "ClassDeclaration" || current.type === "ClassExpression") {
          return current.id?.name || null;
        }
        current = current.parent;
      }
      return null;
    }

    /**
     * Get columns that are already referenced via ${Model.column} in template literal
     */
    function getReferencedColumns(node) {
      const referenced = new Set();
      if (node.expressions) {
        for (const expr of node.expressions) {
          if (expr.type === "MemberExpression" &&
              expr.property?.type === "Identifier") {
            referenced.add(expr.property.name.toLowerCase());
          }
        }
      }
      return referenced;
    }

    /**
     * Check if a position in the string is preceded by JSON operators (->> or ->)
     */
    function isPrecededByJsonOperator(str, position) {
      // Look backwards from position for ->> or ->
      const before = str.slice(Math.max(0, position - 10), position);
      return /->>'?$/.test(before) || /->'?$/.test(before);
    }

    /**
     * Extract potential column names from a string
     */
    function extractPotentialColumns(str) {
      const columns = new Map(); // columnName -> position
      
      // Reset regex lastIndex
      columnPatternBefore.lastIndex = 0;
      columnPatternAfter.lastIndex = 0;
      orderByPattern.lastIndex = 0;

      let match;
      
      // Match column before operator (e.g., "name = ?")
      while ((match = columnPatternBefore.exec(str)) !== null) {
        const col = match[1].toLowerCase();
        const position = match.index;
        // Skip if preceded by JSON operator (e.g., metadata->>source_id)
        if (isPrecededByJsonOperator(str, position)) continue;
        if (!columns.has(col)) {
          columns.set(col, position);
        }
      }

      // Match column after operator (e.g., "= name")
      while ((match = columnPatternAfter.exec(str)) !== null) {
        const col = match[1].toLowerCase();
        const position = match.index + match[0].length - match[1].length;
        // Skip if preceded by JSON operator
        if (isPrecededByJsonOperator(str, position)) continue;
        if (!columns.has(col)) {
          columns.set(col, position);
        }
      }

      // Match ORDER BY / GROUP BY
      while ((match = orderByPattern.exec(str)) !== null) {
        const col = match[1].toLowerCase();
        if (!columns.has(col)) {
          columns.set(col, match.index + match[0].length - match[1].length);
        }
      }

      return columns;
    }

    /**
     * Check if a column name should be warned about
     */
    function shouldWarn(columnName) {
      // Always check specified columns
      if (alwaysCheckColumns.has(columnName)) {
        return true;
      }
      
      // Check minimum length
      if (columnName.length < minColumnNameLength) {
        return false;
      }

      // Skip common SQL keywords that might be mistaken for columns
      const sqlKeywords = new Set([
        "and", "or", "not", "null", "true", "false",
        "asc", "desc", "limit", "offset", "from", "where",
        "select", "insert", "update", "delete", "join",
        "left", "right", "inner", "outer", "cross",
        "case", "when", "then", "else", "end",
        "count", "sum", "avg", "min", "max",
        "like", "ilike", "between", "exists",
        "all", "any", "some", "distinct",
      ]);
      
      if (sqlKeywords.has(columnName)) {
        return false;
      }

      return true;
    }

    /**
     * Get full text content of template literal (static parts only)
     */
    function getTemplateStaticText(node) {
      if (node.quasis) {
        return node.quasis.map(q => q.value.raw).join(" ");
      }
      return "";
    }

    /**
     * Check template literal and report hardcoded columns
     */
    function checkTemplateAndReport(node, modelName) {
      // Get static text from template
      const staticText = getTemplateStaticText(node);
      if (!staticText.trim()) return;

      // Get columns already referenced via ${Model.column}
      const referencedColumns = getReferencedColumns(node);

      // Extract potential column names from static text
      const potentialColumns = extractPotentialColumns(staticText);

      // Report columns that are not referenced
      for (const [columnName, _position] of potentialColumns) {
        // Skip if already referenced via ${...}
        if (referencedColumns.has(columnName)) continue;

        // Skip if doesn't meet criteria
        if (!shouldWarn(columnName)) continue;

        // Report warning
        context.report({
          node,
          messageId: "hardcodedColumnNameWithSuggestion",
          data: {
            columnName,
            suggestion: `\${${modelName}.${columnName}}`,
          },
        });
      }
    }

    /**
     * Check string literal and report hardcoded columns
     */
    function checkStringAndReport(node, modelName) {
      // Extract potential column names
      const potentialColumns = extractPotentialColumns(node.value);

      // Report all potential columns in plain strings
      for (const [columnName, _position] of potentialColumns) {
        if (!shouldWarn(columnName)) continue;

        context.report({
          node,
          messageId: "hardcodedColumnNameWithSuggestion",
          data: {
            columnName,
            suggestion: `\`\${${modelName}.${columnName}}\``,
          },
        });
      }
    }

    return {
      TemplateLiteral(node) {
        // 1. Check condition methods (find, update, etc.)
        let callerModel = getCallerModel(node);
        if (callerModel && /^[A-Z]/.test(callerModel)) {
          checkTemplateAndReport(node, callerModel);
          return;
        }

        // 2. Check raw SQL methods (query, withQuery)
        callerModel = getRawSqlCallerModel(node);
        if (callerModel && /^[A-Z]/.test(callerModel)) {
          checkTemplateAndReport(node, callerModel);
          return;
        }

        // 3. Check static QUERY property
        if (isStaticQueryProperty(node)) {
          const className = getClassNameFromStaticProperty(node);
          if (className && /^[A-Z]/.test(className)) {
            checkTemplateAndReport(node, className);
          }
        }
      },

      // Also check plain string literals
      Literal(node) {
        // Only check string literals
        if (typeof node.value !== "string") return;

        // 1. Check condition methods (find, update, etc.)
        let callerModel = getCallerModel(node);
        if (callerModel && /^[A-Z]/.test(callerModel)) {
          checkStringAndReport(node, callerModel);
          return;
        }

        // 2. Check raw SQL methods (query, withQuery)
        callerModel = getRawSqlCallerModel(node);
        if (callerModel && /^[A-Z]/.test(callerModel)) {
          checkStringAndReport(node, callerModel);
          return;
        }

        // 3. Check static QUERY property
        if (isStaticQueryProperty(node)) {
          const className = getClassNameFromStaticProperty(node);
          if (className && /^[A-Z]/.test(className)) {
            checkStringAndReport(node, className);
          }
        }
      },
    };
  },
};

