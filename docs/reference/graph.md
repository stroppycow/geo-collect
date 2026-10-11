# Graph API

Module `rnipp_geo_data_collector.graph`: the event graph shared by steps
03, 04 and 05.

::: rnipp_geo_data_collector.graph.graph_event

## Role in the pipeline

```mermaid
flowchart LR
    T1[insee_departements] --> G
    T2[insee_communes +<br/>departements + COM] --> G
    T3[insee_communes +<br/>arrondissements] --> G
    G["build_history_graph_of_geo_entities<br/>(directed igraph)"]
    G --> E03[03 · Metropolitan France]
    G --> E04[04 · Typology]
    G --> E05[05 · Postal codes]
```

The graph is directed: dated edges link every entity to its start and end
events. Any historical question ("who existed when", "who descends from
whom") becomes a graph traversal. See the
[technical architecture](../explanation/architecture.md).
