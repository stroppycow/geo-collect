# Update the documentation

The anti-inconsistency principle: **one single source of truth per piece of
information**. The documentation is deliberately split in two spaces:

- the **code** holds the signatures and Google-style docstrings —
  mkdocstrings renders them in the reference pages, without duplication;
- the **step cards** hold the narrative (why, business rules, diagrams)
  and **link** to the reference.

## Checklist before any PR

1. [ ] Docstring of the changed code up to date (Google style) — it
       propagates automatically to the [reference](../reference/cli.md) at
       the next build.
2. [ ] Card of the modified step: "Business rules" and
       "Parameters and influence" tables verified (see the
       [step card template](../explanation/steps/step-card-template.md)).
3. [ ] Mermaid diagrams re-synchronised if the flow changed.
4. [ ] How-to guides updated if the user-visible behaviour changed
       ([exceptions](../how-to/handle-exceptions.md),
       [performance](../how-to/tune-performance.md)).
5. [ ] `poetry run mkdocs build --strict` passes without warnings.

## Add a step to the pipeline

1. Create the card from the
   [step card template](../explanation/steps/step-card-template.md),
   named `NN-step-name.md`.
2. Add the step to `nav` in `mkdocs.yml` ("Step cards" section).
3. Update the overview diagrams:
   [Methodology](../explanation/methodology.md) (decisions table),
   [Architecture](../explanation/architecture.md) (modules, flows).
4. Document the entry function in the
   [processings API](../reference/processings.md) via mkdocstrings
   (`::: rnipp_geo_data_collector.processings....`).

## Build and publish

- Locally: `poetry run mkdocs serve` (preview on
  `http://localhost:8000`) then `poetry run mkdocs build --strict`.
- On GitHub: the `.github/workflows/docs.yml` workflow builds the site on
  every push to `main` and publishes it to GitHub Pages; on PRs, the build
  runs as a plain check.
- A page not listed in `nav` does not appear in the menu: this is the
  guard against orphan pages.

## Style rules

- Language: English; code identifiers (functions, classes, columns)
  stay in English.
- Diagrams: inline mermaid (``` mermaid), no versioned drawio files —
  rendering happens client-side through Material.
- Material admonitions (`!!! abstract`, `!!! warning`) for points of
  attention, used sparingly.
