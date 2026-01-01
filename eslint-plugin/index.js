/**
 * @fileoverview ESLint plugin for litedbmodel
 * @description Provides rules for better litedbmodel usage patterns
 */

"use strict";

const requireDeclareForRelations = require("./rules/require-declare-for-relations");
const noCrossModelColumnInCondition = require("./rules/no-cross-model-column-in-condition");
const preferColumnReference = require("./rules/prefer-column-reference");

module.exports = {
  meta: {
    name: "eslint-plugin-litedbmodel",
    version: "1.0.0",
  },
  rules: {
    "require-declare-for-relations": requireDeclareForRelations,
    "no-cross-model-column-in-condition": noCrossModelColumnInCondition,
    "prefer-column-reference": preferColumnReference,
  },
  configs: {
    recommended: {
      plugins: ["litedbmodel"],
      rules: {
        "litedbmodel/require-declare-for-relations": "error",
        "litedbmodel/no-cross-model-column-in-condition": "error",
        "litedbmodel/prefer-column-reference": "warn",
      },
    },
  },
};

