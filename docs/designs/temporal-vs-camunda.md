# Workflow Orchestration: Temporal vs Camunda

**Status:** In progress  
**Context:** Evaluating workflow orchestration solutions for a production healthcare platform processing prior authorization requests at scale.

## The Problem

We needed a durable workflow orchestration layer to manage long-running, multi-step business processes — prior auth requests that span days or weeks, involve external API calls, human review steps, and require reliable state management through failures and retries.

## Candidates Evaluated

- **Temporal** — code-first orchestration, strong developer ergonomics, Go/Java/Python SDKs
- **Camunda** — BPMN-based, process modeling first, strong enterprise adoption in regulated industries

## Key Tradeoffs

|                             | Temporal       | Camunda         |
| --------------------------- | -------------- | --------------- |
| Workflow definition         | Code (Java/Go) | BPMN XML + code |
| Developer experience        | Excellent      | Moderate        |
| Ops overhead (self-hosted)  | High           | High            |
| Managed offering            | Temporal Cloud | Camunda SaaS    |
| Visibility / auditing       | Good           | Excellent       |
| Enterprise / compliance fit | Moderate       | Strong          |

## Decision

_Full writeup coming soon._

## What I'd Do Differently

_Coming soon._
