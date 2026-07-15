<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Backfill Date Filters — Relevant Files

Diagram of the files touched to add the "Filter by Start Date / End Date"
filter to the Backfills tab ([issue #53049](https://github.com/apache/airflow/issues/53049)).
Solid arrows are data/type flow (a change in the source file requires
regenerating or updating the target); dashed arrows are test coverage.

```mermaid
flowchart TD
    subgraph Backend["Backend (source of truth)"]
        Route["routes/ui/backfills.py\nadds from_date / to_date RangeFilter params"]
        RouteTest["tests/.../ui/test_backfills.py"]
    end

    subgraph Generated["Generated (regenerated, not hand-edited)"]
        Spec["openapi/_private_ui.yaml\n(prek run generate-openapi-spec)"]
        Client["openapi-gen/requests/services.gen.ts\nopenapi-gen/requests/types.gen.ts\nopenapi-gen/queries/*.ts\n(pnpm run codegen)"]
    end

    subgraph FilterWiring["Frontend filter wiring"]
        SearchParams["constants/searchParams.ts\nFROM_DATE_RANGE / TO_DATE_RANGE keys"]
        FilterConfigs["constants/filterConfigs.tsx\nlabel, icon, gte/lte mapping"]
        FiltersHandler["utils/useFiltersHandler.ts\nFilterableSearchParamsKeys union"]
    end

    subgraph BackfillsTab["Backfills tab"]
        FiltersComponent["pages/Dag/Backfills/BackfillsFilters.tsx\nrenders FilterBar"]
        Page["pages/Dag/Backfills/Backfills.tsx\nreads params, calls query hook"]
        PageTest["pages/Dag/Backfills/Backfills.test.tsx"]
    end

    subgraph TestInfra["Frontend test infra"]
        MockHandler["mocks/handlers/backfills.ts"]
        MockIndex["mocks/handlers/index.ts"]
    end

    Route --> Spec
    Spec --> Client
    Client --> Page

    SearchParams --> FilterConfigs
    SearchParams --> FiltersHandler
    FilterConfigs --> FiltersHandler
    FiltersHandler --> FiltersComponent
    FiltersComponent --> Page

    MockHandler --> MockIndex
    MockIndex -.-> PageTest
    Page -.-> PageTest
    Route -.-> RouteTest
```
