# README Review Notes

## Date

2026-06-20

## Summary

The README was reviewed and restructured based on external feedback. The library's positioning was changed from "lightweight TypeScript ORM" to "Production-safe SQL-first Database Access Layer for AI-assisted development."

## Changes Made

1. **Opening description**: Replaced generic ORM positioning with a value proposition centered on production safety and AI-assisted development.
2. **"Why AI-friendly?" section**: Added after Philosophy to explain why predictable SQL, explicit transactions, and hard limits matter when AI agents co-develop production code.
3. **"Production Safety Features" section**: Elevated safety features (transaction-required writes, hard limits, N+1 prevention, reader/writer separation, SQL visibility) into a standalone section.
4. **DX redefinition note**: Added to the Philosophy section to clarify that litedbmodel optimizes for reviewability and safety over traditional ORM convenience.
5. **Comparison table**: Changed framing from feature-by-feature speed comparison to philosophy-first positioning (Abstraction-first vs SQL-first vs SQL-first + production safety).
6. **Security section**: Added near the end of README with warnings about raw SQL escape hatches and a link to SECURITY.md.
7. **SECURITY.md**: Created as a standalone file documenting trust boundaries, unsafe/safe patterns, and vulnerability reporting instructions.

## Rationale

The previous README described litedbmodel accurately but positioned it as a generic ORM alternative. The restructured README highlights the properties that differentiate it in AI-assisted development workflows: predictable SQL output, explicit transaction boundaries, hard safety limits, and full SQL visibility.
