# Study guide template (used at the end of every phase)

When a phase's code works and its tests pass, write
`docs/study/NN-<phase-name>.md` following this template.

## Reader
A 3rd-year CS student. Knows programming and basic SQL. Knows NOTHING about
prediction markets, trading or blockchains.

## Style (strict)
- Plain, simple English. Short sentences.
- Never use a technical word without a one-line plain definition the first time.
- Give an everyday analogy first, then the technical term.
- Use small worked examples with real numbers. Show all arithmetic step by step.
- Explain WHY something exists, not only WHAT it does.

## Accuracy rules
- Describe only the REAL code and REAL tests in the repo. Never describe code that
  does not exist. If the code differs from the plan, say so.
- For anything about Polymarket, its APIs or its contracts, use only
  PROJECT_BRIEF.md and what was observed in docs/decisions. If a detail is not
  there, write "UNVERIFIED - check <official link>". Never invent field names,
  endpoints, addresses, fees or numbers.
- Say nothing about the company Gravia beyond what PROJECT_BRIEF.md states.
- Label every number MEASURED or ESTIMATED.

## Sections (one Markdown file)
1. "In one minute": a short summary of what was built and why.
2. Glossary: term | plain meaning | where it appears in the code.
3. Mental model with an analogy.
4. Step-by-step explanation with worked numeric examples taken from the tests.
5. A diagram (Mermaid or ASCII).
6. File-by-file, function-by-function table:
   name | what it does | inputs | outputs | why it exists.
7. Trace ONE event or request from arrival to final result, naming the real
   functions it passes through.
8. Common mistakes and edge cases (include the ones the tests cover).
9. 10 interview questions with short answers.
10. Teach-back checklist: 5 things I should be able to explain out loud.
11. Further reading: official links only.

Be as deep and complete as possible. Do not skip steps. Length is welcome.
