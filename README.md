# ByteWave
This application allows you to migrate data from one storage service to another seamlessly. Allowing for extra customization along the way if you so choose. :) 

![Main Logo](/assets/logo_large_with_text_overlay.png)

# ByteWave Roadmap (Where We Are & Where We’re Going)

> ByteWave is a local, privacy‑first file migration tool with advanced **Path Verification (PV)** filters, **File Path Validation (FPV)** checks/cleaning, and an event‑driven core. This roadmap is a living snapshot for contributors, users, and curious hiring managers.

---


## Status at a Glance

**Legend:** ✅ done · 🔄 in progress · ⏳ queued/blocked · 🧪 testing/alpha

* ✅ **Signal Router (SR) event system** — implemented; decouples components and resolves missed‑signal deadlocks.
* ✅ **Source traversal (local & network)** — functional.
* 🔄 **Destination traversal** — nearly complete; final logic tweaks and stabilization.
* ⏳ **Upload phase** — next major core milestone (starts once traversal is hardened).
* 🧪 **Early alpha traversal tests** — ongoing.
* ⏳ **“Prepare the Root” UX/API** — replaces the current hardcoded JSON root setup used in dev/tests.
* ✅ **PV vs FPV clarified** — PV = user filters (can stop recursion). FPV = compatibility checks/cleaning (never stops traversal; accumulates findings).

> **Website:** planned later. For now, this README and the wiki are the source of truth.

---


## What’s Blocking What (and main sub-projects ongoing or future)

1. **Finish traversal (src + dst) & stabilize** (main blocker - work in progress though)
2. **Implement upload phase (alpha)** (blocked by #1)
3. **Minimal API** (start/stop/pause/resume, browse/select roots, settings) + **Prepare the Root flow** (not blocked by anything)
4. **Desktop UI skeleton** (folder picker, run, progress/logs) (not fully blocked but will need traversal & upload done before we can really start building most of the core functionality)
5. **Path Review Phase** (UI + API, between traversal and upload) (blocked by basic UI & API building and blocked by destination traversal needing to be finished)
6. **Cloud providers** (OAuth, listing, uploads) (blocked by dynamic resource allocation system changes)
7. **PV polish & advanced rule sets** (not blocked by anything but we do need a basic UI to build the part of the UI building the config jsons that it parses)
8. **FPV polish** (provider‑specific rules, batch clean UX) (semi-blocked, parts of this can be done now. See the FPV issue in the issues page for more info)
9. **Installers** (cross platform) (can be worked on now for basic installation of the go distributable exe but should be looked at it again just before we get to beta testing phase of the software)
10. **reporting dashboards** (blocked by core traversal & upload functionality)

**Notes**

* **Prepare the Root** = UI folder picker + API endpoints to browse the local FS, select src/dst roots, and persist them to DB; replaces hardcoded JSON “root” entries. Every stored path is relative to its selected root.
* **API/UI** follow traversal+upload so the UI has meaningful controls from day one.
* **Cloud support** depends on API/UI (OAuth device/user flows and listing views).

---


## Near‑Term Core Priorities

1. **Destination traversal: finish & harden**

   * Queue events, edge cases, DB writes; behavior symmetric with source traversal.

2. **Upload phase (alpha)**

   * Streaming I/O (no giant buffers), retries/back‑off, resumability, structured error recording.

3. **Prepare the Root** (API + basic UI)

   * API: `GET /fs/roots`, `GET /fs/ls?path=…`, `POST /roots` (set src/dst), `GET /roots`.
   * UI: simple folder picker → show chosen src/dst → **Start traversal**.

---


## Path Review Phase (between traversal and upload) (after destination traversal is finished)

> FPV can progress now, but this phase unlocks FPV’s full value.

**Goals**

* Display *everything* traversal found: included items, PV‑skipped, FPV violations, failures.
* Enable users to:

  * Mass‑filter extra files/folders (ad‑hoc filters beyond PV).
  * Re‑queue failures or **re‑run traversal**.
  * Batch‑apply **FPV clean** (auto‑fix names) and preview destination‑safe mappings **before** upload.

**MVP**

* API: paged listings with filters; actions to include/exclude and re‑queue; "clean+map" preview; create an **Upload Plan**.
* UI: virtualized table, quick filters (type/status/reason), bulk select/apply, “Generate Upload Plan.”

**Later**

* Diff view (original vs cleaned), conflict resolution, undo/rollback.

---


## Side Projects You Can Start Now (Unblocked)

> Open today; no deep orchestration knowledge required.

* **PV (Path Verification / filter rules)**

  * *What:* Rule schema, parser, evaluator during traversal (e.g., type/size/date/path rules).
  * *Behavior:* PV can **stop recursion** under disallowed folders to speed traversal.
  * *Good first tasks:* rule JSON schema + examples; evaluator skeleton; unit tests; perf sanity checks on large trees.

* **FPV (File Path Validation / compatibility & cleaning)**

  * *What now:* Implement `isValid` + `clean` interfaces (start with common OS rules; add provider rules later).
  * *Behavior:* FPV **does not stop traversal**; it records findings on each path and suggests transforms.
  * *Dependency to fully finish:* **Path Review Phase** (batch clean/map/ignore UX & APIs).
  * *Good first tasks:* spec FPV contract (req/resp), implement `isValid` & `clean`, tests for edge cases, persist suggested transforms.

* **Desktop UI and API skeleton (limited)**

  * Shell screens: Home, Root Picker, Run, Progress/Logs. Start with mock/minimal API responses; wire to real endpoints as they land.

* **Types library 📚**

  * Shared types/schemas for API ↔ UI contracts; generate clients where helpful; keep endpoints and UI in lockstep.

* **Test scaffolding**

  * Fixtures, golden files, high‑variance edge cases. 

> **Installer:** groundwork (notes/templates) is fine, but final packaging should wait until CLI/API/configs stabilize. Expect this near beta.

---


## Performance & Architecture

* **Signal Router (SR):** centralized topic based message queue (in memory, ephemeral with buffering options but no persistence - (topics like `queue:running_low`, `traversal:complete`) - eliminates channel spaghetti and missed signals.
* **Dynamic Resource Allocation (DRA):** adjusts worker pools for traversal/upload in response to CPU/mem/I/O/network and external rate limits—maximizes throughput without thrashing.

---


## Security & Privacy

* **Local‑only by default** — no phone‑home.
* **Planned** — optional local auth (protects against rogue launches), later MFA; least‑privilege tokens for cloud providers; transparent docs on what’s stored locally.

---


## Rough Timeline (very rough; subject to change)

* **0–3 months** — traversal hardening (src/dst), upload (alpha), Prepare the Root API + minimal UI.
* **+3–9 months** — fleshed‑out API, UI skeleton → early UX, first cloud provider(s), PV polish, initial reporting, installer groundwork.
* **+3 months** — heavy beta (stability, recovery drills, performance, automated tests), FPV batch clean UX, docs pass → **V1**.

Parallel contributions can accelerate this. See **Side Projects** above to get started.

---


## How to Contribute

* **Intermediate Go:** FS browse endpoints (Prepare the Root), settings CRUD operations, FPV `isValid/clean` stub + tests.
* **Soft Skills:** We will need user facing documentation written out once most of this is done, as well as maybe some content creation for a little hype and to kickstart our general user base. Nothing major like business level stuff, just some basic things to give us that little extra kicker and smoother landing.
* **Graphic & Sound Design** We will need 3D renderings, sound design sorts of skills for the animated 3D scene backgrounds, some beautiful artwork, and good sound design for a truly immersive software experience. Skills in Unity or Unreal Engine are probably best here for the artwork but could theoretically be done in any tool as it will be pre-rendered content! 
* **Advanced backend:** upload workers (streaming, retries, resume), traversal edge cases & DB schema, provider adapters (Drive/Dropbox/OneDrive).
* **UI/UX:** folder selection modal, progress/logs panel, Path Review table (bulk actions, virtualization).

Open an issue to claim something, or jump into one labeled **good first issue**. PRs welcome.

---

