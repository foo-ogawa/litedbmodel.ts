/**
 * @fileoverview Require 'declare' keyword for relation properties instead of '!' assertion
 * @description Relations use prototype getters. Using '!' creates instance properties that shadow getters.
 */

"use strict";

/** @type {import('eslint').Rule.RuleModule} */
module.exports = {
  meta: {
    type: "problem",
    docs: {
      description: "Require 'declare' keyword for relation properties decorated with @hasMany, @belongsTo, or @hasOne",
      category: "Possible Errors",
      recommended: true,
    },
    fixable: "code",
    schema: [],
    messages: {
      useDeclare:
        "Relation property '{{name}}' should use 'declare' instead of '!' assertion. " +
        "Class field declarations with '!' create instance properties that shadow the prototype getter.",
    },
  },

  create(context) {
    const relationDecorators = new Set(["hasMany", "belongsTo", "hasOne"]);
    const sourceCode = context.getSourceCode();

    /**
     * Check if a decorator is a relation decorator
     */
    function isRelationDecorator(decorator) {
      // Handle @hasMany(...) - CallExpression
      if (decorator.expression?.type === "CallExpression") {
        const callee = decorator.expression.callee;
        // Direct call: @hasMany(...)
        if (callee.type === "Identifier" && relationDecorators.has(callee.name)) {
          return true;
        }
      }
      // Handle @hasMany without parentheses (unlikely but possible)
      if (decorator.expression?.type === "Identifier") {
        return relationDecorators.has(decorator.expression.name);
      }
      return false;
    }

    /**
     * Check if property has relation decorator
     */
    function hasRelationDecorator(node) {
      const decorators = node.decorators || [];
      return decorators.some(isRelationDecorator);
    }

    /**
     * Get the relation decorator name for error message
     */
    function getRelationDecoratorName(node) {
      const decorators = node.decorators || [];
      for (const decorator of decorators) {
        if (decorator.expression?.type === "CallExpression") {
          const callee = decorator.expression.callee;
          if (callee.type === "Identifier" && relationDecorators.has(callee.name)) {
            return callee.name;
          }
        }
        if (decorator.expression?.type === "Identifier" && relationDecorators.has(decorator.expression.name)) {
          return decorator.expression.name;
        }
      }
      return null;
    }

    return {
      // Check PropertyDefinition (class fields) - TypeScript AST
      PropertyDefinition(node) {
        // Skip if no decorators or not a relation
        if (!hasRelationDecorator(node)) {
          return;
        }

        // Check if using definite assignment assertion (!)
        // In TypeScript ESLint AST, this is represented as `definite: true`
        if (node.definite === true) {
          const propertyName = node.key?.name || node.key?.value || "unknown";
          
          context.report({
            node,
            messageId: "useDeclare",
            data: {
              name: propertyName,
            },
            fix(fixer) {
              // Get the source text of the property
              const text = sourceCode.getText(node);
              
              // Find the property name and the ! after it
              // Pattern: propertyName!: Type
              const match = text.match(/^(\s*(?:@\w+\([^)]*\)\s*)*?)(\w+)(!)(:\s*.+)$/s);
              if (match) {
                // Replace propertyName!: with declare propertyName:
                const [, decorators, name, , typeAndRest] = match;
                const newText = `${decorators}declare ${name}${typeAndRest}`;
                return fixer.replaceText(node, newText);
              }
              
              // Alternative: just replace the node key area
              // Find the ! token after the property key
              const tokens = sourceCode.getTokens(node);
              for (let i = 0; i < tokens.length; i++) {
                const token = tokens[i];
                if (token.type === "Punctuator" && token.value === "!") {
                  // Find if there's a 'declare' keyword already
                  const hasDeclare = tokens.some(t => t.type === "Keyword" && t.value === "declare");
                  if (!hasDeclare) {
                    // Remove ! and add declare before property name
                    const keyToken = tokens.find(t => t.type === "Identifier" && t.value === propertyName);
                    if (keyToken) {
                      return [
                        fixer.insertTextBefore(keyToken, "declare "),
                        fixer.remove(token),
                      ];
                    }
                  }
                  break;
                }
              }
              
              return null;
            },
          });
        }
      },

      // Also check ClassProperty for older ESLint versions
      ClassProperty(node) {
        // Delegate to PropertyDefinition handler
        this.PropertyDefinition(node);
      },
    };
  },
};

