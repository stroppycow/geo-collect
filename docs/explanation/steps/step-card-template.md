# Step card template

Every pipeline step is documented by a **standardised card** serving the
three reading levels. To add a new step, copy this template into
`docs/explanation/steps/` and fill in every section.

```markdown
# Step NN — <Short name>

!!! abstract "Summary"
    One sentence: what the step does, on which inputs, to which output.

## 1. Methodological level

### Goal
What the step brings to the end user.

### Why this step
The business rule, the upstream constraint, the rejected alternative.

### Parameters and influence
Table: parameter → effect on the result when it changes.

### Position in the process
A mermaid `flowchart` (before / after).

## 2. Functional level

### Implementation
The module, the entry function, and an mkdocstrings link to the API.

### Business rules
Table: rule identifier → wording → source (code).

### Inputs / Outputs / Dependencies
Tables of read and written tables, the registry, the modules.

### Detailed behaviour
A mermaid `sequenceDiagram` or `flowchart` of the internal processing.

## 3. Technical level

### Change the behaviour
The extension point, the pattern to follow.

### Test the change
Minimal command to check the impact.

### Update the documentation
The sections to re-synchronise (card, docstring, reference).
```

## Consistency rules

1. **One narrative per step**: the card carries the *why* and the *what*;
   signatures and docstrings stay in the code and are rendered by
   mkdocstrings in the reference pages — never duplicate them.
2. **Mermaid over prose** as soon as a flow exceeds three elements.
3. **Every quoted business rule must be traceable** to a source file
   (`path/to/module.py`).
4. After modifying a step, tick the checklist in
   [Update the documentation](../../contributor/update-documentation.md).
