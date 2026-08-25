# Migrationsplan: TrueNAS-Normalisierung von coordinator.py nach aiotruenas

## Context

Copilot hat auf PR #179103 (Bronze-Fork `kayl-codes/core`, Branch `truenas_ce`) bemängelt, dass
`coordinator.py` durch die ~500-Zeilen-`apiparser.py` plus endpoint-spezifische Normalisierung/
State-Derivation faktisch eine zweite Domain-Library ist statt eines dünnen HA-Wrappers. Ein
analoges, kleineres Finding (Exception-Klassifizierung, TLS-Warning-Filter) wurde bereits per
aiotruenas v1.2.0 behoben (`_errors.py` + `quiet=`-Parameter). Dieses Finding zielt auf die
komplette Normalisierungs-Engine.

Die Recherche (3 parallele Explore-Agents über aiotruenas, `apiparser.py` und beide
`coordinator.py`-Varianten) liefert eine wichtige Korrektur der ursprünglichen Annahme: **Bronze
und Prod haben identische Endpoint-Abdeckung** — alle 23 `get_*`-Normalizer existieren in beiden
Repos. Der Unterschied liegt nicht in den Endpoints, sondern ausschließlich in HA-spezifischer
Infrastruktur *um* dieselben Normalizer herum:

- **Prod-only**: Websocket-Push-Subscription-Layer (`_ensure_push_subscription`,
  `_on_*_push`/`stop_*_push`, `_PushSourceState`, Circuit-Breaker) und Orphaned-Statistics/
  Migration-Issue-Tracking (`issue_registry`/`recorder`-gekoppelt, Platinum-Feature).
- **Beide identisch**: die eigentlichen `parse_api(...)`-Aufrufe, die Feldspezifikationen
  (`_POOL_VALS`, `_JOB_PROGRESS_VALS` etc.) und die echte Derived-State-Logik
  (`_apply_pool_errors`/`_apply_pool_capacity`, `_accumulate_vdev_errors`/
  `_aggregate_topology_errors`, `_netdata_mean_value`/`_arc_value`/`_ups_value`,
  Replication-State-Fallback, Certificate-Expiry).

Das vereinfacht Frage 2 des Nutzers erheblich: Die Library muss keinen Endpoint-Superset
verwalten, den ein Consumer nicht braucht — beide brauchen exakt dieselben Normalizer. Nur die
HA-Orchestrierung bleibt zwangsläufig pro Repo lokal.

`apiparser.py` selbst (549 Zeilen) ist bereits reines, HA-unabhängiges Stdlib-Python mit
vollständiger Testabdeckung (`tests/test_apiparser.py`, 606 Zeilen) — der ideale erste
Migrationsschritt. Als Architektur-Präzedenzfall existiert bereits `exceptions.py` (reine
Datenklassen, öffentlich) + `_errors.py` (private Klassifizierungslogik, schmale
Entry-Point-Funktionen, eigene Tests) aus dem v1.2.0-Refactor — dieses Muster soll die neue
Normalisierungsschicht wiederholen.

## Zielarchitektur

**Neues Subpackage `src/aiotruenas/domain/`** (erstes Subpackage im bisher flachen Layout —
gerechtfertigt durch den Umfang von ~19 Endpoints):

- `domain/_normalize.py` — 1:1-Port von `apiparser.py`: `parse_api`, `generate_keymap`,
  `ApiValueSpec`, `fill_vals`/`fill_ensure_vals`/`fill_vals_proc`, `utc_from_timestamp`,
  `human_date_to_utc` usw. Bleibt **privat** (Implementierungsdetail der Domain-Schicht, kein
  Endnutzer-API) — analog zu `_errors.py`. Tests 1:1 aus `test_apiparser.py` übernommen.
- `domain/_helpers.py` — die geteilten reinen Rechenhelfer: `_median`, `_accumulate_vdev_errors`,
  `_aggregate_topology_errors`, `_netdata_mean_value`/`_arc_value`/`_ups_value`,
  `_stat_name_similar`, `_as_int`/`_to_int`. Ebenfalls privat, eigene Tests.
- `domain/_specs.py` — die Feldspezifikationstabellen (`_POOL_VALS`, `_JOB_PROGRESS_VALS`,
  `_JOB_STATUS_VALS`, `_CERTIFICATE_VALS`, …), 1:1 aus coordinator.py übernommen.
- `domain/state.py` — **neue öffentliche Klasse `TrueNASState`**, komponiert einen
  `TrueNASClient` (nicht Vererbung — hält `TrueNASClient` schlank und transport-fokussiert, wie
  heute). Besitzt das `self._ds`-Äquivalent (ein `dict[str, dict[str, Any]]` je Endpoint,
  exakt im heutigen `coordinator.ds[...]`-Format) und pro Endpoint eine `async def get_pool()`
  usw. — Naming bewusst identisch zu den heutigen Coordinator-Methoden, um den Umstieg in beiden
  Consumer-Repos auf ein simples "ruft jetzt `state.get_pool()` statt lokalem `parse_api(...)`
  auf" zu reduzieren.
- Rückgabetyp in dieser Phase: **Dicts im heutigen `ds[...]`-Format**, keine Dataclasses. Das
  minimiert die Änderungsfläche (Sensoren/Entities in beiden Consumer-Repos lesen weiterhin
  `coordinator.ds["pool"][guid]["name"]` etc.) und trennt die Normalisierungs-Migration sauber
  von einer möglichen späteren Typisierungs-Migration (eigener, späterer Schritt).
- `__init__.py` exportiert `TrueNASState` zusätzlich zu `TrueNASClient` — additiv, keine
  Breaking Change an der bestehenden `call()`-Oberfläche. Wer nur `call()` will, ist von der
  neuen Schicht komplett unberührt.

**Was in coordinator.py (beide Repos) bleibt:**
- `DataUpdateCoordinator`-Lifecycle (`_async_update_data`, `_async_ensure_connected`).
- Die gesamte Push-Subscription-Orchestrierung (`_PushSourceState`, `_on_*_push`,
  Circuit-Breaker) — das ist HA-Update-Listener-Koordination, keine TrueNAS-Normalisierung.
- Recorder-/Issue-Registry-Housekeeping (orphaned statistics, migration-rollback issues).
- App-Update-Job-State-Machine, soweit sie an HA-Update-Entities hängt.
- Optimistic-State-Handling (`set_optimistic_running`) und `async_run_task`.

coordinator.py ruft künftig `self.state = TrueNASState(self.api.client)` auf und ersetzt jeden
lokalen `parse_api(...)`-Block durch `await self.state.get_pool()` etc.; `self.ds` wird entweder
zu `self.state`'s internem Dict oder bleibt eine dünne Referenz darauf.

**Nutzer-Ergänzung bei Planfreigabe:** `TrueNASState` darf beim Zugriff auf den zugrunde
liegenden `TrueNASClient` nicht auf ein privates Attribut wie `self.api._client` zugreifen.
Der jeweilige `TrueNASAPI`-Wrapper in beiden Consumer-Repos muss dafür eine **öffentliche
Property** (z. B. `TrueNASAPI.client`) bereitstellen, über die `TrueNASState` den `TrueNASClient`
erhält. Betrifft die Consumer-seitige Anbindung ab Schritt 3, ist aber jetzt als verbindliche
Designentscheidung festgehalten.

## Migrationsschritte (inkrementell, mehrere PRs)

Begründung für inkrementell statt Big Bang: öffentlich auf PyPI publizierte Library, zwei
unabhängige Consumer-Repos mit eigenen Release-Zyklen, ~19 Endpoints mit stellenweise
nicht-trivialer Derived-State-Logik (Vdev-Fehleraggregation, Pool-Capacity-Herleitung aus
Datasets). Ein einzelner Umbau wäre schwer reviewbar und schwer rückrollbar.

1. **Schritt 1 — Engine-Port** (`aiotruenas` PR, Version 1.3.0): `apiparser.py` 1:1 nach
   `domain/_normalize.py`, Tests mitnehmen. **Portierungsquelle ist explizit die Prod-Version**
   `d:\#Projekte\GIT\homeassistant-truenas\custom_components\truenas_ce\apiparser.py` (HACS-Repo),
   nicht die ha-core-Fork-Kopie — beide sind laut Recherche inhaltlich identisch (nur
   Formatierung/Kommentare unterscheiden sich), aber die Prod-Version gilt als Source of Truth.
   Kein Consumer-seitiger Change nötig, rein additiv. Kleinster, risikoärmster Schritt — schafft
   die Grundlage für alles Weitere.
2. **Schritt 2 — Shared Helpers** (gleiche oder Folge-PR): `domain/_helpers.py` mit den
   Netdata/Vdev/Median-Funktionen + Tests.
3. **Schritt 3 — Pilot-Endpoints** (Version 1.4.0): `TrueNASState` mit `get_pool()` und
   `get_dataset()` (vom Nutzer selbst als Beispielpaar genannt) plus einem
   Job-Progress-Endpoint (`get_cloudsync()`, deckt `_JOB_STATUS_VALS`-Wiederverwendung ab).
   Danach **Validierung im Bronze-Fork**: PR #179103 auf den neuen Aufruf umstellen, echte
   TrueNAS-Instanz testen (analog zum bestehenden `test/live-verification`-Setup), bevor
   weitergemacht wird.
4. **Schritt 4+ — restliche Endpoints in thematischen Batches**, je eigener PR +
   Minor-Bump: (a) Jobs (replication/rsync/snapshottask/cronjob), (b) Stats
   (systemstats/arc/ups/disk-temp), (c) Services/VM/Container/App, (d)
   Alerts/Certificates/Directoryservices. Jeder Batch wird im Bronze-Fork verpflichtend
   nachgezogen; die Übernahme im Prod-HACS-Repo kann parallel/nachgelagert im eigenen
   Release-Rhythmus erfolgen, da die Library ab Schritt 1 bereits den vollen (identischen)
   Endpoint-Bedarf beider Repos abdeckt.

Jeder Schritt: normaler Feature-Branch-PR-Workflow in aiotruenas (fix→extend→package), synchron
in `pyproject.toml` + `__init__.py` `__version__`. **Versionierung (Nutzer-Entscheidung
2026-08-25):** Schritte 2–4 laufen unter Patch-Bumps (1.3.1, 1.3.2, …); erst mit Abschluss von
Schritt 4 (alle Endpoints migriert) erfolgt ein gemeinsamer Minor-Bump auf 1.4.0. Kein
Commit/Push/PR ohne explizite Freigabe.

### Status (2026-08-25)

- ✅ Schritt 1 — Engine-Port: PR #15, gemerged, v1.3.0.
- ✅ Schritt 2 — Shared Helpers: PR #16, gemerged, v1.3.1.
- 🔧 Schritt 3 — Pilot-Endpoints (`get_pool()`/`get_dataset()`/`get_cloudsync()` auf
  `TrueNASState`) in Arbeit, v1.3.2. Dabei nachgezogen: `prune`/`_prune_stale_uids` in
  `_normalize.py` — im Prod-`apiparser.py` seit Commit `e06b7ff` (2026-08-25, Copilot-Review auf
  PR #179103) vorhanden, fehlte im Schritt-1-Port; jetzt 1:1 nachportiert (siehe Kritische
  Dateien). Bronze-Fork-Validierung gegen echte TrueNAS-Instanz noch offen.

## Zu klärende/entscheidende Punkte (Antworten auf die 3 Nutzerfragen)

1. **1:1-Normalizer vs. feste Rückgabetypen** → Hybrid: generischer `parse_api`-Kern wandert
   1:1 (privat) als Fundament; darauf aufbauend feste, benannte Methoden pro Endpoint
   (`get_pool()`, `get_dataset()`, …) mit Dict-Rückgabe im heutigen Format — kein rohes
   `parse_api()` als Endnutzer-API.
2. **Rückwärtskompatibilität/Endpoint-Superset** → entfällt in der ursprünglich befürchteten
   Form: Bronze und Prod brauchen laut Recherche identische Endpoints. Die Library deckt ab
   Schritt 1 den vollen gemeinsamen Bedarf; kein Consumer verliert Code.
3. **Inkrementell vs. Big Bang** → inkrementell, siehe oben — mit Pool+Dataset+einem
   Job-Endpoint als Pilot vor dem Rest.

## Kritische Dateien

- `d:\#Projekte\GIT\aiotruenas\src\aiotruenas\__init__.py` — neuer Export `TrueNASState`.
- `d:\#Projekte\GIT\aiotruenas\src\aiotruenas\exceptions.py` / `_errors.py` — Referenzmuster für
  öffentlich/privat-Trennung.
- Quelle für den Port: `d:\#Projekte\GIT\homeassistant-truenas\custom_components\truenas_ce\apiparser.py`
  (als Vorbild, nicht kopieren wg. Lizenz-Historie — aber da beide Repos eigene, unter welcher
  Lizenz auch immer stehende Kopien haben und dies eine Eigenentwicklung ist, ist eine
  Übernahme in aiotruenas als Neuimplementierung/Verschiebung im Sinne des Nutzers zu behandeln,
  nicht als Fremdcode-Import wie bei der LGPL-Klausel in PROMPT.md — das im ersten
  Umsetzungs-PR kurz gegenchecken).
- Feldspezifikationen/Helfer-Vorbild: `d:\#Projekte\GIT\homeassistant-truenas\custom_components\truenas_ce\coordinator.py`
  (Zeilen 74–169 Specs, 175–436 Shared Helpers, 1530–1789 Pool/Dataset als Referenzimplementierung).

## Verifikation

- Jeder Engine-/Helper-Port: `ruff check . && ruff format --check . && pytest` in aiotruenas,
  wie im bestehenden CI-Workflow.
- Pilot-Endpoints (Schritt 3): zusätzlich echte Verifikation gegen eine laufende TrueNAS-Instanz
  (bestehendes `examples/verify_live.py`-Muster aus dem Live-Verification-Spike), da
  Feldspezifikationen sich auf reale API-Shapes verlassen.
- Nach jedem Consumer-seitigen Umstieg (Bronze-Fork PR #179103 zuerst): bestehende
  Coordinator-Tests weiterhin grün, keine Regression bei Sensor-Werten (manueller Vergleich
  alter vs. neuer `ds[...]`-Inhalt für mind. einen Pool und ein Dataset).
