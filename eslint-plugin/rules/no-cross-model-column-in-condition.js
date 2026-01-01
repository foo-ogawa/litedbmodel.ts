/**
 * @fileoverview Disallow using columns from different models in find/update/delete conditions
 * @description Catches cases like User.find([[`${Post.id} = ?`, 1]]) where Post column is used in User query
 */

"use strict";

/** @type {import('eslint').Rule.RuleModule} */
module.exports = {
  meta: {
    type: "problem",
    docs: {
      description: "Disallow using columns from a different model in find/update/delete conditions",
      category: "Possible Errors",
      recommended: true,
    },
    schema: [],
    messages: {
      crossModelColumn:
        "Column '{{column}}' belongs to model '{{columnModel}}', but expected '{{expectedModel}}'. " +
        "Did you mean to use '{{expectedModel}}.{{columnName}}'?",
      crossModelColumnInSubquery:
        "Column '{{column}}' belongs to model '{{columnModel}}', but subquery target is '{{targetModel}}'. " +
        "Conditions should use columns from '{{targetModel}}'.",
      wrongParentColumnInSubquery:
        "First column in key pair '{{column}}' belongs to '{{columnModel}}', but this is a '{{callerModel}}' query. " +
        "First column should be from '{{callerModel}}'.",
    },
  },

  create(context) {
    // Methods that take conditions as first argument
    const conditionMethods = new Set([
      "find",
      "findOne",
      "findFirst",
      "findById",
      "update",
      "delete",
      "count",
    ]);

    // Subquery methods that use key pairs
    const subqueryMethods = new Set([
      "inSubquery",
      "notInSubquery",
    ]);

    // EXISTS methods (conditions determine target model)
    const existsMethods = new Set([
      "exists",
      "notExists",
    ]);

    // Methods that legitimately use cross-model columns (skip checking)
    const crossModelMethods = new Set([
      "query",           // Raw SQL - can reference any table
    ]);

    /**
     * Extract model name from a MemberExpression like User.id or User.find
     */
    function getModelName(node) {
      if (node.type === "MemberExpression" && node.object?.type === "Identifier") {
        return node.object.name;
      }
      return null;
    }

    /**
     * Extract column info from a MemberExpression like User.id
     * Returns { modelName, columnName } or null
     */
    function getColumnInfo(node) {
      if (node.type === "MemberExpression" && 
          node.object?.type === "Identifier" &&
          node.property?.type === "Identifier") {
        const modelName = node.object.name;
        // Check if it looks like a model name (PascalCase)
        if (/^[A-Z]/.test(modelName)) {
          return {
            modelName,
            columnName: node.property.name,
          };
        }
      }
      return null;
    }

    /**
     * Check if a node is inside a parentRef() call
     */
    function isInsideParentRef(node) {
      let current = node.parent;
      while (current) {
        if (current.type === "CallExpression" &&
            current.callee?.type === "Identifier" &&
            current.callee.name === "parentRef") {
          return true;
        }
        current = current.parent;
      }
      return false;
    }

    /**
     * Check template literal expressions for cross-model columns
     */
    function checkTemplateLiteralColumns(node, expectedModel, messageId) {
      if (!node.expressions) return;

      for (const expr of node.expressions) {
        // Skip if inside parentRef()
        if (isInsideParentRef(expr)) continue;

        const columnInfo = getColumnInfo(expr);
        if (columnInfo && columnInfo.modelName !== expectedModel) {
          const sourceCode = context.getSourceCode();
          const columnText = sourceCode.getText(expr);

          context.report({
            node: expr,
            messageId,
            data: {
              column: columnText,
              columnModel: columnInfo.modelName,
              expectedModel: expectedModel,
              targetModel: expectedModel,
              columnName: columnInfo.columnName,
            },
          });
        }
      }
    }

    /**
     * Check condition array elements for cross-model columns
     */
    function checkConditionArray(node, expectedModel, messageId) {
      if (node.type !== "ArrayExpression") return;

      for (const element of node.elements) {
        if (!element) continue;

        // Check if element is a condition tuple [column/string, value]
        if (element.type === "ArrayExpression" && element.elements?.length >= 1) {
          const firstElement = element.elements[0];
          
          // Check template literal: [`${User.id} = ?`, value]
          if (firstElement?.type === "TemplateLiteral") {
            checkTemplateLiteralColumns(firstElement, expectedModel, messageId);
          }
          
          // Check direct column reference: [User.id, value]
          if (firstElement?.type === "MemberExpression") {
            // Skip if inside parentRef
            if (!isInsideParentRef(firstElement)) {
              const columnInfo = getColumnInfo(firstElement);
              if (columnInfo && columnInfo.modelName !== expectedModel) {
                const sourceCode = context.getSourceCode();
                const columnText = sourceCode.getText(firstElement);

                context.report({
                  node: firstElement,
                  messageId,
                  data: {
                    column: columnText,
                    columnModel: columnInfo.modelName,
                    expectedModel: expectedModel,
                    targetModel: expectedModel,
                    columnName: columnInfo.columnName,
                  },
                });
              }
            }
          }
        }
        
        // Recursively check nested arrays (for OR conditions, etc.)
        // But skip if it looks like a key pair [[col, col], ...]
        if (element.type === "ArrayExpression" && 
            element.elements?.[0]?.type !== "MemberExpression") {
          checkConditionArray(element, expectedModel, messageId);
        }
      }
    }

    /**
     * Extract target model from key pairs in inSubquery/notInSubquery
     * Key pairs format: [[ParentCol, TargetCol], ...] or [ParentCol, TargetCol]
     */
    function getSubqueryTargetModel(keyPairsArg) {
      if (keyPairsArg?.type !== "ArrayExpression") return null;
      
      const firstElement = keyPairsArg.elements[0];
      if (!firstElement) return null;

      // Check if it's composite: [[col, col], ...]
      if (firstElement.type === "ArrayExpression") {
        const targetCol = firstElement.elements?.[1];
        if (targetCol) {
          const info = getColumnInfo(targetCol);
          return info?.modelName || null;
        }
      }
      // Single pair: [col, col]
      else if (firstElement.type === "MemberExpression") {
        const targetCol = keyPairsArg.elements[1];
        if (targetCol) {
          const info = getColumnInfo(targetCol);
          return info?.modelName || null;
        }
      }
      
      return null;
    }

    /**
     * Check key pairs in inSubquery/notInSubquery
     * First column should be from caller model, second from target model
     */
    function checkSubqueryKeyPairs(keyPairsArg, callerModel) {
      if (keyPairsArg?.type !== "ArrayExpression") return;
      
      const firstElement = keyPairsArg.elements[0];
      if (!firstElement) return;

      // Check if it's composite: [[col, col], ...]
      if (firstElement.type === "ArrayExpression") {
        for (const pair of keyPairsArg.elements) {
          if (pair?.type !== "ArrayExpression") continue;
          const parentCol = pair.elements?.[0];
          if (parentCol?.type === "MemberExpression") {
            const info = getColumnInfo(parentCol);
            if (info && info.modelName !== callerModel) {
              const sourceCode = context.getSourceCode();
              context.report({
                node: parentCol,
                messageId: "wrongParentColumnInSubquery",
                data: {
                  column: sourceCode.getText(parentCol),
                  columnModel: info.modelName,
                  callerModel,
                },
              });
            }
          }
        }
      }
      // Single pair: [col, col]
      else if (firstElement.type === "MemberExpression") {
        const info = getColumnInfo(firstElement);
        if (info && info.modelName !== callerModel) {
          const sourceCode = context.getSourceCode();
          context.report({
            node: firstElement,
            messageId: "wrongParentColumnInSubquery",
            data: {
              column: sourceCode.getText(firstElement),
              columnModel: info.modelName,
              callerModel,
            },
          });
        }
      }
    }

    return {
      CallExpression(node) {
        // Check for Model.method()
        if (node.callee?.type !== "MemberExpression") return;
        
        const methodName = node.callee.property?.name;
        const callerModel = getModelName(node.callee);
        
        if (!callerModel || !/^[A-Z]/.test(callerModel)) return;

        // Skip raw SQL methods
        if (crossModelMethods.has(methodName)) return;

        // Check find/update/delete conditions
        if (conditionMethods.has(methodName)) {
          const conditionsArg = node.arguments[0];
          if (conditionsArg) {
            checkConditionArray(conditionsArg, callerModel, "crossModelColumn");
          }
        }

        // Check inSubquery/notInSubquery
        if (subqueryMethods.has(methodName)) {
          const keyPairsArg = node.arguments[0];
          const conditionsArg = node.arguments[1];

          // Check key pairs - first column should be from caller model
          checkSubqueryKeyPairs(keyPairsArg, callerModel);

          // Check conditions - should use target model columns
          const targetModel = getSubqueryTargetModel(keyPairsArg);
          if (targetModel && conditionsArg) {
            checkConditionArray(conditionsArg, targetModel, "crossModelColumnInSubquery");
          }
        }

        // Check exists/notExists
        if (existsMethods.has(methodName)) {
          // For exists, conditions determine target model - get from first condition
          const conditionsArg = node.arguments[0];
          if (conditionsArg?.type === "ArrayExpression" && conditionsArg.elements[0]) {
            const firstCond = conditionsArg.elements[0];
            if (firstCond?.type === "ArrayExpression") {
              const firstCol = firstCond.elements?.[0];
              if (firstCol?.type === "MemberExpression") {
                const targetInfo = getColumnInfo(firstCol);
                if (targetInfo) {
                  // Check all conditions use the same target model
                  checkConditionArray(conditionsArg, targetInfo.modelName, "crossModelColumnInSubquery");
                }
              }
            }
          }
        }
      },
    };
  },
};
