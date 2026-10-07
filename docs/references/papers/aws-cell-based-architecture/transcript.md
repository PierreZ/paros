# Reducing the Scope of Impact with Cell-Based Architecture

**Publisher:** AWS Well-Architected (Amazon Web Services)
**Contributor:** Robisson Oliveira, Sr. Cloud Application Architect, AWS
**Publication date:** September 20, 2023. Document History lists only "Initial publication" at
that date; no later revision is recorded. (The PDF's cover-page copyright stamp reads 2026, an
artifact of when the PDF was rendered/fetched, not a content revision.)
**Source (PDF):** <https://docs.aws.amazon.com/pdfs/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/reducing-scope-of-impact-with-cell-based-architecture.pdf>
**Source (HTML):** <https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/what-is-a-cell-based-architecture.html>

---

## Abstract (the whitepaper's summary, in full)

*"Today, modern organizations face an increasing number of challenges related to resiliency, be
they scalability or availability, especially when customer expectations shift to an always on,
always available mentality. More and more, we have remote teams and complex distributed
systems, along with the growing need for frequent launches and an acceleration of teams,
processes, and systems moving from a centralized model to a distributed model. All of this
means that an organization and its systems need to be more resilient than ever.*

*With the increasing use of cloud computing, sharing resources efficiently has become easier.
The development of multi-tenant applications is increasing exponentially, but although the use
of the cloud is more understood and customers are aware that they are in a multi-tenant
environment, what they still want is the experience of a single-tenant environment.*

*This guidance aims to demonstrate how to increase the resilience of critical applications,
bringing the same fault isolation concepts that AWS applies in its Availability Zones and
Regions to the level of your workload architecture. It expands one of the best practices from
the AWS Well-Architected Framework, Use bulkhead architectures to limit scope of impact, to help
you reduce the effect of failures to a limited number of components."*

---

## 1. Introduction

Resilience is a workload's ability to respond to and quickly recover from failures. AWS gives
customers several isolation boundaries (Availability Zones, Regions, control planes, data
planes) but has, **for more than a decade**, also used **cell-based architecture** internally to
build more resilient and scalable services. Cell-based architecture gives a workload more
**fault isolation, predictability, and testability**, properties the paper frames as fundamental
for extreme-resilience applications.

**Are you Well-Architected?** A one-paragraph pointer to the six pillars of the AWS
Well-Architected Framework and the free AWS Well-Architected Tool; not specific to cells.

## 2. Shared Responsibility Model for Resiliency

A short section locating this guidance: AWS is responsible for "Resiliency of the Cloud"; the
workload architecture discussed here, including cell-based architecture, is the customer's
responsibility, "Resiliency in the Cloud." Cells are an **additional** level of fault isolation
layered on top of, not a replacement for, the Availability Zones and Regions AWS already
provides.

## 3. What is a cell-based architecture?

**The bulkhead analogy.** A cell-based architecture is modeled on a ship's bulkheads: vertical
partition walls that subdivide the hull into self-contained, watertight compartments so a breach
floods only one compartment. *(Figure, described: a cross-section of a ship's hull showing
vertical bulkhead walls creating separate watertight compartments.)* In software, a **fault
isolated boundary** restricts a failure's effect to a limited set of components; components
outside the boundary are unaffected.

**The core mechanism.** The overall workload is partitioned by a **partition key** chosen to
align with "the grain of the service" (the natural way a workload subdivides with minimal
cross-cell interaction). Examples given are customer ID or resource ID, or any parameter easily
accessible in most API calls. A **cell routing layer** distributes requests to individual cells
based on the partition key and presents a single endpoint to clients.

**Definition.** "A cell-based architecture uses multiple isolated instances of a workload, where
each instance is known as a cell. Each cell is independent, does not share state with other
cells, and handles a subset of the overall workload requests." **If a workload uses 10 cells to
service 100 requests, a failure in one cell leaves 90% of overall requests unaffected.** Cells
contain failure types that are otherwise hard to contain: unsuccessful code deployments, and
requests that are corrupted or invoke a specific failure mode, i.e. **poison pill requests**.

**3.1 A typical workload.** *(Figure, described: a monolithic three-layer application,
presentation, application, data, serving 100% of clients from one stack.)* In this topology, a
failure or a bad change affects 100% of customers.

**3.2 A workload with cell-based architecture.** Rather than one monolithic image, break the
service into cells with a thin routing layer in front; this can be zonal, regional, or global.
*(Figure, described: a cell router fanning requests out to several complete copies of the
three-layer stack, each copy one cell.)* Three components recur through the rest of the paper:

- **Cell router**: "the thinnest possible layer," whose only job is routing requests to the
  right cell.
- **Cell**: a complete workload, with everything needed to operate independently.
- **Control plane**: administration (provisioning cells, de-provisioning cells, migrating cell
  customers).

Adopting cells need not mean multiplying infrastructure: an application with 30 hosts can stay
at 30 hosts, with a cell router added and the hosts grouped/distributed between cells.

## 4. Why use a cell-based architecture?

- **Scale-out over scale-up.** Scaling up grows a component's size; scaling out grows the
  *number* of components so load per component stays bounded. Scale-out is harder to divide
  (especially for stateful systems) but gives: **workload isolation** (containment of
  deployment failures, poison pills, misbehaving clients, data corruption, operational
  mistakes), **maximally-sized components** (capped size avoids non-linear scaling surprises),
  and **not too big to test** (a capped component can be stress-tested past its breaking point;
  this doesn't by itself test the whole scaled-out system, but if most of the risk sits in the
  stress-tested component, confidence rises). *(Figure, described: scale-out via adding
  identically-sized cells once a region exceeds one cell's capacity.)*
- **Lower scope of impact.** Properly isolated cells give failure containment "similar to what
  we see with Regions": **"It's highly unlikely for a service outage to span multiple Regions.
  It should be similarly unlikely for a service outage to span multiple cells."**
  *(Figure, described: 10 cells, one failing, 90% of customers unaffected.)*
- **Higher scalability, or cells as a unit of scale.** Knowing and testing a cell's capacity lets
  you scale by adding cells rather than growing one instance, staying under
  account/service/Region limits. *(Figure, described: growth handled by adding new cells rather
  than enlarging existing ones.)*
- **Higher testability.** A capped cell size makes maximum-scale behavior testable; it's
  impractical to simulate an entire multi-tenant service's load, but reasonable to simulate the
  largest workload a single cell (matching the largest single customer) can see.
- **Higher mean time between failure (MTBF).** Consistent, capped, regularly-tested cell size
  removes "every day is a new adventure"; spreading customers across cells plus gradual
  deployment contains code/traffic spikes to some cells while others stay stable, raising average
  time between failures.
- **Lower mean time to recovery (MTTR).** Cells limit the number of hosts to analyze/touch during
  diagnosis and emergency fixes; predictable size and scale make recovery more predictable.
- **Higher availability.** "A system with n cells will have n times as many failure events, but
  each with 1/n of the impact." Combined with higher MTBF and lower MTTR, cells give fewer,
  shorter failures per cell and higher overall availability, including by the definition
  `#successful requests / #total requests`. Cells minimize the time the numerator is zero.
- **More control over the impact of deployments and rollbacks.** Like one-box and Single-AZ
  deployments, cells add a dimension for phasing deployments. The first cell in a phased rollout
  can be a **canary cell**, and each cell can run its own canary with synthetic/non-critical
  traffic, layering a canary strategy on top of even-smaller blast radii.

## 5. When to use a cell-based architecture?

Workloads that benefit:

- Applications where any downtime has a huge negative impact on customers.
- FSI (financial-services) customers with workloads critical to economic stability.
- Ultra-scale systems too big/critical to fail.
- **Less than 5 seconds of Recovery Point Objective (RPO).**
- **Less than 30 seconds of Recovery Time Objective (RTO).**
- Multi-tenant services where some tenants require fully dedicated tenancy (their own dedicated
  cell).

The paper's framing question: **"Is it better for 100% of customers to experience a 5% failure
rate, or 5% of customers to experience a 100% failure rate?"**

Cells are not a good choice for every workload. Stated disadvantages: increased architectural
complexity from redundant infrastructure/components; higher infrastructure/service cost
(mitigated by utilization pricing such as EC2 Reserved Instances and savings plans); need for
specialized operational tooling to run multiple replicas; and the necessity of investing in a
cell routing layer.

## 6. Implementing a cell-based architecture

### 6.1 Control plane and data plane

Borrowed from networking/routers: the **data plane** moves traffic per rules (here: the cell
router plus the cells doing the service's actual work); the **control plane** creates and
distributes those rules (provisioning, moving, migrating, updating, removing, deploying,
monitoring cells).

**Static stability.** Citing Well-Architected **REL11-BP04, "Rely on the data plane and not the
control plane during recovery"**: in a statically stable design the system keeps working even
when a dependency is impaired: here, the **data plane keeps serving even if the control plane
is down, or even if an Availability Zone is also down**. Control planes are statistically more
likely to fail than data planes; the data plane depends on data the control plane produced but,
once resources are provisioned, has no further dependency on the control plane, so it is
unaffected by control-plane impairment (you can't create/modify/delete, but existing resources
stay available). The paper also maps this to CAP: **control planes favor CP** (designed to fail
rather than corrupt or return wrong information), **data planes generally favor AP** (try to
stay available even on stale information). Worked example: when a compute-layer cell router
loads its map from S3, routes stay in memory, so **the router keeps directing traffic even if
the control plane, S3, or a zone is unavailable**.

### 6.2 Cell design

A cell is an instance of the complete workload (e.g., a load balancer, EC2 instances, and an RDS
database together form one cell; a second cell duplicates all three). How you draw a cell's
boundary materially affects resiliency, cost, and architecture. **Ideally a cell is independent,
unaware of other cells, and shares no state with them**: no cross-cell API calls, no shared
databases or S3 buckets, even separate AWS accounts are encouraged. Cross-cell dependencies
quickly erode the architecture's benefits, so keep them minimal or only at specific transitory
times.

**Multi-AZ cells.** Each cell spans multiple AZs or a whole Region, inheriting AWS's own Regional
/Multi-AZ resiliency and availability, and keeps running for its clients even if one AZ is
unavailable. *(Figure, described: one cell's resources spread across several AZs within a
Region.)*
- *Advantages:* wider use of Regional serverless services; easier for a cell to be self-resilient
  via serverless/managed Multi-AZ services **without sharing state with external components**.
- *Disadvantages:* less control over an AZ failure, particularly **gray failures** (some
  components unstable). Evacuating an isolation zone (an AZ or a Region) may not fix a gray
  failure.

**Single-AZ cells.** Cells follow AZ boundaries directly, suited to services that already expose
AZ as a failure unit (the paper's example: EC2, where customers choose an AZ and are encouraged
to tolerate a single AZ's failure). *(Figure, described: three cells, each confined to one AZ,
each needing its own zonal router endpoint.)*
- *Advantages:* lets you accurately detect which AZ has a problem and mitigate it specifically.
- *Disadvantages:* **requires three cell routers** and clients choosing the right zonal endpoint;
  requires services with AZ scope in their configuration; requires extra DR mechanisms
  (active-passive or active-active) to keep cell resiliency, and **replicating cell state to
  another cell, which "can break the cell concept."**

**Should a Single-AZ cell fail over if an AZ becomes unavailable, or on a gray failure?**
**Cells are implemented primarily to limit the scope of a failure's impact**, mainly cascading
failures caused by excessive resource load and problematic/buggy deployments, **not to mitigate
dependency failures or single points of failure. "Therefore, they were not designed as failover
domains."** Worked example: four cells down from one AZ outage affects roughly a third of
customers (scaled to the chosen cell size). Two options given: (1) accept that cells limit
scope, not failover, and let the fault be tolerated per its own isolation scope; or (2) combine
Single-AZ cells with more traditional DR mechanisms: each Single-AZ cell gets one or more
replicas in other AZs via a replication layer whose strategy depends on the stateful component
(RDS, DynamoDB, ElastiCache, Kinesis, SQS, see the *Disaster Recovery of Workloads on AWS*
whitepaper for pilot-light/warm-standby/active-active). *(Figure, described: a Single-AZ cell's
replica in a second AZ taking over traffic when the first AZ fails.)* This second path is more
complex and markedly more expensive; if your workload doesn't need execution scoped to one AZ,
**Multi-AZ cells are the better default.**

### 6.3 Cell partition

Cell partition is how traffic is divided across cells via a (possibly composite) **partition
key**, chosen to match the service's grain with minimal cross-grain interaction, and easily
accessible in most API calls. A key consideration is maximum cell size: `CustomerID` looks like a
natural key, but a single customer that outgrows one cell needs either a second dimension in the
partition key (aligned to the business) or a dedicated allocation. Interactions that cut against
the grain (e.g., scatter-gather) are inevitable but should be a minority; route any cross-cell
calls back through the normal cell router rather than letting cells call each other directly.

Any partitioning algorithm needs (a) a mechanism to serve/distribute the state the algorithm
uses, and (b) graceful handling of migration as cells are added/removed. A non-exhaustive,
non-prescriptive list of mapping algorithms:

- **Full mapping**: explicitly map every key to a cell. *(Figure, described: a table with one
  row per key, mapping each to its cell.)* *Advantages:* simple; most control over distribution,
  good for hot-cell control and migration. *Disadvantages:* a critical read/write dependency on
  the mapping table, a read-your-writes consistency requirement, a large amount of state;
  high performance cost at high cardinality; an in-memory map can mean a longer router bootstrap.
- **Prefix and range-based mapping**: map ranges of keys (or key hashes) to cells, offsetting
  full mapping's downsides. *(Figure, described: contiguous ranges of keys grouped and mapped to
  cells.)* *Advantages:* reduces full mapping's performance cost by grouping keys, cutting
  cardinality. *Disadvantages:* more likely to produce a hot cell, since there's no control over
  which keys within a range draw the most traffic.
  - **Naive modulo mapping (fixed partition number)**: modular arithmetic, typically over a
    cryptographic hash of the key. *Advantages:* effectively zero peak-to-average ratio (very
    even spread), minimal state (just the cell count); simple; avoids hot cells.
    *Disadvantages:* **changing the cell count requires rebalancing all cells and their
    customers/tenants** (high churn).
  - **Consistent hashing**: a family of algorithms mapping keys to buckets (cells) with small,
    fairly stable state and minimal churn on add/remove. The paper names the **Ring Consistent
    Hash (Karger et al.)**, as used by the Chord DHT, noting it can suffer high peak-to-average
    ratios (uneven spread), offsettable with more state; and two newer algorithms that improve
    on both axes: **"A Fast, Minimal Memory, Consistent Hash Algorithm" (Lamping and Veach)** and
    **"Multi-probe consistent hashing" (Appleton and O'Reilly)**. A common cell-mapping pattern:
    configure a fixed, large number (e.g., tens of thousands) of logical buckets explicitly
    mapped to a much smaller number of physical cells; mapping a key is then two steps: naive
    modulo to a logical bucket, then a bucket-to-cell table lookup. *Advantages:* changing cell
    count does not require rebalancing all cells/tenants; simple. *Disadvantages:* can still
    suffer significant peak-to-average unevenness.
  - **A warning for all mapping approaches.** Regardless of algorithm, keep an **override table**
    to force specific keys to specific cells (full mapping gets this natively), useful for
    testing, quarantining, and special-casing heavy partition keys. Also: mapping a new customer
    to a cell and registering it in the router is a **control-plane** task; only after that
    provisioning does the chosen routing strategy start applying to that key.

### 6.4 Cell routing

The router is the one **shared** component across cells, so it cannot follow the same
compartmentalization strategy cells do. Recommendation: distribute requests via a computationally
efficient partition-mapping algorithm, e.g. combining cryptographic hashing with modular
arithmetic. To avoid multi-cell impact, the router must stay as simple and horizontally scalable
as possible, avoiding complex business logic (citing Colm MacCárthaigh's *"Reliability, constant
work, and a good cup of coffee"* on how simple, constant-work designs are more reliable and less
fragile). Router features to keep in mind: **be as simple as possible, but not simpler**; isolate
request dispatch between cells; minimize business logic in this layer; abstract cellular
complexity from clients; be fast and reliable; **keep operating normally for other cells when one
cell is unreachable.** *(Figure, described: the router consulting its mapping state before
dispatching to the target cell.)* Four (non-exhaustive) router designs, plus a resilience note:

- **Amazon Route 53**: DNS routing with health checks; the Route 53 data plane SLA is **100%**.
  Give each tenant a custom DNS record pointed at its assigned cell; can combine with **Route 53
  Application Recovery Controller** for AZ/Region failover or gray-failure evacuation.
  *(Figure, described: per-tenant DNS records resolving to cell endpoints.)*
- **Amazon API Gateway**: a serverless, Regional REST/HTTP/WebSocket service with native AWS
  integrations, caching, throttling, rate limiting, canary deployments, usage plans; paired with
  **DynamoDB** (single-digit-millisecond latency, 99.99%/99.999% SLA with global tables) for the
  mapping lookup, optionally accelerated by **DynamoDB Accelerator (DAX)** (up to **10×** faster,
  milliseconds to microseconds, even at millions of requests/sec). Any database can substitute
  for DynamoDB depending on workload needs. *(Figure, described: API Gateway in front of a
  DynamoDB-backed cell map.)*
- **A compute layer (EC2/ECS/EKS/Lambda) + S3**: the control plane writes the cell mapping to an
  S3 bucket; the router's only job is to inspect request data and pick the target cell. The
  mapping lives **in memory** on the router, kept current by a listener process/thread that
  updates the map when the S3 object changes. Which approach fits best depends on access-pattern
  complexity, partition-key cardinality, and cell count; the paper points to *"Avoiding overload
  in distributed systems by putting the smaller service in control"* for synchronization patterns
  between data plane and control plane. *(Figure, described: a compute-layer router with an
  in-memory map refreshed from S3.)*
- **Routing non-HTTP requests**: nothing restricts the router to HTTP; it can be an
  event/message broker, e.g. an Amazon SQS-based payment-request flow where the router consumes
  the request topic and dispatches to the correct cell, same in-memory-map-from-S3 pattern as
  above. *(Figure, described: an SQS/MSK-fed router dispatching events to cells.)*
- **About resilience of the cell router.** The router is the **only component holding shared
  state across all cells**, a single point of failure in effect, so it must be built for
  **maximum reliability** and, regardless of AZ-dependent or AZ-independent cell strategy, treated
  as a cellular component itself for service limits, size, and observability. The routing layer
  still has to scale infinitely, but the problems to solve for scaling "the thinnest possible
  layer" should be a **subset** of what a non-cellularized application would face.

### 6.5 Cell sizing

Cell-based architectures benefit from a **capped maximum cell size**, consistent across
installations (AZs/Regions). Three opposing forces when choosing size: big enough to fit the
largest workloads; small enough to test at full scale/operate efficiently (lower risk of scaling
cliffs, staying below account limits); big enough to gain economies of scale. The optimum varies
per service and customer behavior and "needn't be extremely large for any service." Trade-off
table (reproduced from the paper):

| | Smaller cells | Larger cells |
|---|---|---|
| Cell count | More cells to deploy and operate; more replicas to manage. | Fewer cells to deploy; fewer replicas to manage. |
| Outage/drain impact | Impacts a small percentage of the compute fleet. | Impacts a larger percentage of the compute fleet. |
| Scaling limits | Less likely to hit scaling limitation; unseen/unknown implementation limits that only appear at scale have reduced impact. | More likely to reach Region/account scaling limitation; larger cells use more compute power. |
| Scope of impact | Reduced scope of impact (e.g. 10 cells → 10% each; 100 cells → 1% each). | Reduced splits: fewer client workloads need to be split across cells. |
| Testability | Easier and cheaper to test to stipulated limits/quotas. | Easier to operate (operating 5 replicas vs. 10, 20, 30), though tooling to automate operations still matters at scale, up to tens or hundreds of cells. |
| Idle capacity | Less idle capacity (lower computational capacity per cell). | Better capacity utilization (larger cells support more clients/traffic, more economy of scale). |

A cell's benefit as a **unit of scale** comes from knowing its maximum limit (citing **REL01-BP01,
"Aware of service quotas and constraints"**): TPS a cell can handle, number of customers/tenants
it supports, GB/s of transfer or stored capacity it supports.

**Define your scaling dimensions.** Tightly related to the partition key: it determines which
cell gets the traffic/storage. The most obvious dimension is client ID, but a single client
growing past a cell's limit (e.g. a cell rated at 10K TPS) forces either scale-up (if possible) or
inability to serve that customer, hence defining **more than one scale-unit dimension** to
handle true outliers, up to giving them one or more **dedicated cells**. Dedicated cells also open
a commercial path to single-tenancy as a paid option, though the scatter/gather routing a
multi-cell customer needs is its own added complexity. *(Figure, described: scaling dimensions
beyond a single partition key, e.g. customer ID plus a business-aligned second axis.)* Cost is
also a factor in sizing, shaping how multi-tenant the system is and the resulting economics.

**Know and respect your cell's limit.** Cell sizing is tied to the traffic limit a cell can bear
without degrading its customers. **Load shedding** to avoid overload is fundamental, informed by
load testing and chaos engineering; **API Gateway** supports rate limiting at the API (resource
and method) and stage level, and a custom router can implement **token bucket** algorithms (the
paper cites the EC2 API as a token-bucket example).

### 6.6 Cell placement

A control-plane responsibility: onboarding tenants/customers and creating cells. Needs
observability into: capacity per cell, used capacity per cell, usage percentage per
tenant/customer, and quotas/limits of each cell within its AWS account. Per **REL01-BP06**:
**"Ensure that a sufficient gap exists between the current quotas and the maximum usage to
accommodate failover."** For data/state-heavy workloads, allocation becomes a scheduling and
forecasting discipline: your strategy for evenly spreading traffic, and for migrating tenants
once one starts to dominate a cell. Factors beyond new-tenant onboarding: cell dimensions
(static or changing), partition-key dimensions (changing over time), the cost of moving a
partition key between cells, and the benefit of co-tenanting/affinity between certain partition
keys. *(Figure, described: the control plane distributing tenants across cells by capacity and
usage.)*

### 6.7 Cell migration

Cells share no state or components, so moving data/customers between cells needs a **migration
strategy** (e.g. when a customer or resource outgrows its cell and needs a dedicated one).
Stateful cell-based architectures will almost certainly need **online** cell migration to adjust
placement as cells are added/removed, including handling mapping decisions mid-transition
(cross-cell redirects, or running multiple iterations of the mapping algorithm against different
versions of its state, or both). The migration itself is system-dependent but typically has these
phases (the paper's exact wording):

1. **Clone** the data from the current location into the new location, as a non-authoritative copy.
2. **Flip** the new location's copy to be authoritative.
3. **Redirect** from the old location to the new location.
4. **Forget** the data at the old location.

An alternative is careful router/cell coordination, using the control plane to migrate clients
from cell to cell and confirming the state transition before the cell is ready for traffic,
which keeps cross-cell dependencies to a minimum (since such dependencies, if they existed, would
already reduce fault isolation).

### 6.8 Cell deployment

Going cellular multiplies what you deploy and operate: instead of one workload instance, you now
have tens, hundreds, or thousands of cell instances across your environments. An automated CI/CD
pipeline is "essential" from the start (the paper points to Amazon's own continuous-delivery
practice, *"My CI/CD pipeline is my release captain"*). *(Figure, described: an Amazon
service-style phased regional rollout, stage by stage to general availability.)* Cell deployment
follows the same phased idea but **cell by cell or set of cells instead of Region by Region**:
**"the important point here is to deploy in waves, cell by cell or set of cells"**, regardless of
whether the cell strategy is AZ-independent or AZ-dependent: deploy to one or more cells at a
time, watch for failure signs, and roll back to limit how many customers were exposed.
*(Figure, described: the same wave-based rollout applied per cell instead of per Region.)*
Pointers into the Well-Architected Reliability and Operational Excellence pillars (as printed,
verbatim, including an apparent duplicate title text between two codes):

- **REL08-BP05** Deploy changes with automation
- **OPS05-BP10** Fully automate integration and deployment
- **OPS06-BP01** Plan for unsuccessful changes
- **OPS06-BP07** Fully automate integration and deployment
- **OPS06-BP08** Automate testing and rollback

Named AWS services that help: **AWS CodeCommit, AWS CodeBuild, AWS CodePipeline.**

### 6.9 Cell observability

Going cellular needs heavy automation and specific tooling; since cell composition varies by
business, much of this must be built in-house, observability included. **The whole observability
stack needs to be cell-aware**: you must be able to track each request and identify which cell it
targets. *(Figure, described: dashboards presenting metrics broken out per cell.)* Pointers to the
Amazon Builders' Library: *"Instrumenting distributed systems for operational visibility"* and
*"Building dashboards for operational visibility"* (both describe Amazon's general practice, not
cell-specific). The central point: do this **at a cell-by-cell level**, giving you a new dimension
to observe and react to.

## 7. Best practices

- **Your current instance/stack is your cell zero.** When planning a migration to cells, treat
  the existing stack as cell zero; add the router layer above it and gradually distribute traffic
  per your cell and partition strategy.
- **Start with multiple cells from day one.** Since a cell is a replica handling a portion of
  customers, start with more than one from day one, since it surfaces the operational issues and
  experience of running this model early, reducing surprises later.
- **Start with a cell migration mechanism from day one.** Migrating clients between cells is
  tricky and workload-dependent; build the mechanism up front so a customer that quickly outgrows
  a cell, or an initial cell size that turns out wrong, can still be moved.
- **Perform a failure mode analysis of your cell.** Since a cell is a failure isolation boundary,
  analyze which services compose it and the effect of each component failing partially or fully,
  confirming other cells stay unaffected. A worksheet of component, cause, probability, and
  mitigation is suggested as a starting exercise.

## 8. Conclusion

Cell-based architecture can raise a workload's isolation, predictability, and testability, but
its trade-offs matter: not every workload needs extreme resiliency. The paper also flags that it
covered workload architecture only: **operational excellence** is a separate concern that gets
harder with cells (dozens or hundreds of workload replicas to operate and evolve), and the
Well-Architected Framework's operational-excellence best practices deserve reinforced attention
for workloads built this way.

## 9. FAQ

**What about shuffle-sharding?** Shuffle-sharding is an excellent fault-isolating mechanism but
is **not** the same thing as cell-based architecture. Its basic idea is to deal shards like hands
of cards: the paper's example takes eight instances, previously split into four disjoint shards
of two instances each; with shuffle-sharding, shards instead contain two **random** instances and,
like hands of cards, may overlap. *(Figure, described: eight instances dealt into overlapping
two-instance shards versus four disjoint two-instance shards.)* Inside a cell-based architecture,
shuffle-sharding can be used **within** a cell (which stays self-contained, sharing no state), but
"cross-cells should not be used by definition." It is also noted as trickier for stateful
components. Two further-reading links given: *"Workload isolation using shuffle-sharding"* and
*"Shuffle Sharding: Massive and Magical Fault Isolation."*

## 10. Contributors, further reading, and document history

**Contributor:** Robisson Oliveira, Sr. Cloud Application Architect, AWS.

**Further reading:** the Well-Architected Reliability Pillar whitepaper, specifically
**REL10-BP04, "Use bulkhead architectures to limit scope of impact"**; the AWS Architecture
Center; the Shared Responsibility Model for Resiliency section of the *Disaster Recovery of
Workloads on AWS* whitepaper; the *AWS Fault Isolation Boundaries* whitepaper; and **"Millions of
Tiny Databases"** (the Amazon paper on Physalia, AWS's configuration-management service for EBS,
built on a cell-like architecture).

**Videos (the paper's closest thing to case studies):**
- re:Invent 2018, *"How AWS Minimizes the Blast Radius of Failures" (ARC338)*.
- *"Physalia: Cell-based Architecture to Provide Higher Availability on Amazon EBS."*
- re:Invent 2022, *"Camada Zero: A real-world architecture framework" (PRT268)*.

**Document history:** one entry, "Initial publication: Whitepaper first published," September
20, 2023. No later revisions are recorded.

---

## Why this matters for `paros`

`docs/architecture.md` already cites this whitepaper directly (§10, "Cells and static
stability," and inline in §3.5/§3.7); checked against the actual text, **every paraphrase there
is faithful**, no mismatch found:

- §3.7's **"a cell is a blast-radius boundary for bad deploys, overload and poison pills, not a
  failover domain"** matches the whitepaper's own wording closely: cells are implemented
  "primarily to limit the scope of a failure's impact," mainly against "excessive load of
  resources and deployments with problems or bugs," and the whitepaper explicitly names **poison
  pill requests** and states plainly **"they were not designed as failover domains."**
- §3.7's **"multi-AZ cells avoid replicating between cells"** matches the whitepaper's own
  trade-off: Multi-AZ cells inherit AWS's built-in Multi-AZ resiliency "without sharing state with
  external components," whereas the Single-AZ path is the one that needs a cross-AZ *replication
  layer* between cells for DR, which the whitepaper says "can break the cell concept." This is the
  same choice `docs/architecture.md` §3.7 and §5 make: a paros cell spans several AZs for its own
  quorums (so Phase 2 survives one AZ loss), rather than failing over to a sibling cell.
- §3.5's **"the thinnest possible router routing on its cached map, [staying up] while the control
  plane is down"** matches the whitepaper's cell-router definition ("the thinnest possible layer,
  with the responsibility of routing requests to the right cell, and only that") and its static
  stability example (a router with an in-memory map fed from S3 keeps directing traffic "even if
  the control plane, Amazon S3 or a zone is unavailable"). paros's frontend does more than that
  (naming, `Authz`); the thin routing layer is a separate **resolver** role (decided 2026-10-07,
  #233), which matches the whitepaper's thinnest-router guidance.
- The sources list's **"migration as copy, flip, redirect, forget"** quotes the whitepaper's own
  four-phase list almost verbatim (clone/flip/redirect/forget), and §3.7's M12 "Moving a tenant"
  paragraph reuses the same four words for exactly the same sequence (reconfigure onto the target
  cell, transfer ownership with `SetLeader`, flip the directory pointer, then the old cell forgets
  the tenant).
- **Not yet reflected in `docs/architecture.md`, but relevant for M12 (#232/#233):** the
  whitepaper's **cell sizing** trade-off table (§6.5 above) and its **partition-key / mapping**
  taxonomy (full mapping, prefix/range, naive modulo, consistent hashing, plus the "always keep an
  override table" warning) are a direct match for the open "tenant → cell" placement question in
  M12: today the fleet directory is a full mapping (every `TenantId` explicitly entered), which
  the whitepaper flags as simple and controllable but with a read/write dependency on the mapping
  table and a cost that grows with cardinality; this is fine at paros's current scale (one fleet
  tenant, few tenants) but is the kind of choice M12 should make explicitly rather than by
  default, should the fleet directory's cardinality ever matter.
- The whitepaper's **static stability** framing (REL11-BP04, cited directly by
  `docs/architecture.md` via the sibling Builders' Library reference) is the same property
  `docs/architecture.md` §1 and §5 build in from M9: the data plane (an existing journal, already
  placed) keeps serving writes/reads with no control-plane dependency even if the fleet or cell
  coordinator is down; only administration (provisioning, tenant creation) needs the control
  plane.
- **Not cited by `docs/architecture.md` today:** the whitepaper's **cell deployment** guidance
  ("deploy in waves, cell by cell or set of cells," with the first wave as a canary cell) and its
  **cell observability** guidance (the whole stack must be cell-aware, tracking which cell a
  request lands in) describe operational practices paros has not yet written down for its own
  cells; worth a forward pointer from §3.7 or the M12 milestone entry once cell deployment and
  per-cell `parosctl status` views are built out, but out of scope for this transcript.
- The FAQ's **shuffle-sharding** distinction ("an excellent fault-isolating mechanism, but not the
  same thing [as cells]... can be used within a cell, but cross-cells should not be used by
  definition") is a useful boundary to keep in mind if paros ever considers shuffle-sharding
  *inside* a cell (e.g. spreading a tenant's roles across a shuffled subset of a cell's machines):
  the whitepaper's position is that doing so stays compatible with the cell-isolation property,
  but using it *across* cells would not.
